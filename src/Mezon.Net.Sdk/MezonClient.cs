using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Mezon.Net.Client;
using Mezon.Net.Client.Messaging;
using Mezon.Net.Core;
using Mezon.Net.Logging;
using Mezon.Net.Models;
using Mezon.Net.Sdk.Agent;
using Mezon.Net.Sdk.Caching;
using Mezon.Net.Sdk.Entities;
using Mezon.Net.Sdk.Managers;

namespace Mezon.Net.Sdk
{
    public sealed partial class MezonClient : IAsyncDisposable
    {
        internal readonly AsyncEvent<Func<LogMessage, Task>> _logEvent = new AsyncEvent<Func<LogMessage, Task>>();
        public event Func<LogMessage, Task> Log { add { _logEvent.Add(value); } remove { _logEvent.Remove(value); } }

        private readonly Client.MezonClient _engine;
        private readonly DmChannelManager _dmChannels = new DmChannelManager();
        private readonly ChannelSendQueue _sendQueue = new ChannelSendQueue();
        private bool _cacheListenersBound;
        private AgentSseManager? _agentManager;
        internal readonly Logger _logger;

        private readonly SemaphoreSlim _initializeGate = new SemaphoreSlim(1, 1);
        private TaskCompletionSource<bool>? _firstConnectTcs;
        private CancellationToken _connectCancellationToken;
        private bool _readyInvoked;

        public MezonClient(MezonClientOptions options)
        {
            Options = options ?? throw new ArgumentNullException(nameof(options));
            _engine = new Client.MezonClient(options);
            _engine.Log += async msg => await _logEvent.InvokeAsync(msg).ConfigureAwait(false);
            _engine.Connected += EngineConnectedHandlerAsync;
            _logger = _engine.LogManager.CreateLogger("MezonSdkClient");
            Clans = new EntityCache<Clan>(options.CacheCapacity);
            Channels = new EntityCache<Channel>(options.CacheCapacity);
            Roles = new EntityCache<Role>(options.CacheCapacity);
            Users = new EntityCache<Entities.User>(options.CacheCapacity);
            ApiClient.RequestQueue.SetRateLimitBypassMessage(SendRateLimitBypassMessageAsync);
        }

        public MezonClientOptions Options { get; }
        internal Client.MezonClient Engine => _engine;
        internal Client.MezonApiClient ApiClient => (Client.MezonApiClient)_engine.ApiClient;
        internal DmChannelManager DmChannels => _dmChannels;
        internal ChannelSendQueue SendQueue => _sendQueue;
        public EntityCache<Clan> Clans { get; }
        public EntityCache<Channel> Channels { get; }
        public EntityCache<Role> Roles { get; }
        public EntityCache<Entities.User> Users { get; }

        public long BotId => Options.BotId;
        public ConnectionState ConnectionState => _engine.ConnectionState;
        public long Latency => _engine.Latency;

        /// <summary>Session auth token after login.</summary>
        public string AuthToken => _engine.CurrentSession.AuthToken;

        /// <summary>Session username after login.</summary>
        public string? Username => _engine.CurrentSession.Username;

        /// <summary>Returns a non-expired session JWT, refreshing when needed.</summary>
        public Task<string> GetAuthTokenAsync() => _engine.GetOrRefreshAuthTokenAsync();

        /// <summary>
        /// Returns the current Mezon session id after refreshing the session when needed.
        /// Suitable for services, such as STN, that accept the gateway SID as a credential.
        /// </summary>
        public async Task<string> GetSessionIdAsync()
        {
            await _engine.GetOrRefreshAuthTokenAsync().ConfigureAwait(false);
            return _engine.CurrentSession.SessionId;
        }

        public event Func<Task> Ready
        {
            add { _readyEvent.Add(value); }
            remove { _readyEvent.Remove(value); }
        }

        private readonly AsyncEvent<Func<Task>> _readyEvent = new AsyncEvent<Func<Task>>();

        public async Task<bool> LoginAsync(CancellationToken cancellationToken = default)
        {
            if (Options.BotId == 0 || string.IsNullOrWhiteSpace(Options.Token))
            {
                throw new ArgumentException("BotId and Token are required.");
            }

            if (!await _engine.LoginAsBotInternalAsync(Options.BotId, Options.Token, autoRefreshSession: true).ConfigureAwait(false))
            {
                return false;
            }

            _connectCancellationToken = cancellationToken;
            _readyInvoked = false;
            _firstConnectTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            await _engine.ConnectAsync().ConfigureAwait(false);
            await AwaitFirstConnectAsync(_firstConnectTcs.Task, cancellationToken).ConfigureAwait(false);

            return true;
        }

        private static async Task AwaitFirstConnectAsync(Task<bool> connectTask, CancellationToken cancellationToken)
        {
            if (!cancellationToken.CanBeCanceled)
            {
                await connectTask.ConfigureAwait(false);
                return;
            }

            var cancelWait = new TaskCompletionSource<bool>();
            using (cancellationToken.Register(state => ((TaskCompletionSource<bool>)state!).TrySetCanceled(), cancelWait))
            {
                var completed = await Task.WhenAny(connectTask, cancelWait.Task).ConfigureAwait(false);
                await completed.ConfigureAwait(false);
            }
        }

        private async Task EngineConnectedHandlerAsync()
        {
            await _initializeGate.WaitAsync(_connectCancellationToken).ConfigureAwait(false);
            try
            {
                await InitializeAfterConnectedAsync(_connectCancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _firstConnectTcs?.TrySetException(ex);
                await _logger.ErrorAsync("Failed to initialize after socket connected.", ex).ConfigureAwait(false);
                return;
            }
            finally
            {
                _initializeGate.Release();
            }

            _firstConnectTcs?.TrySetResult(true);

            if (!_readyInvoked)
            {
                _readyInvoked = true;
                if (_readyEvent.HasSubscribers)
                {
                    await _readyEvent.InvokeAsync().ConfigureAwait(false);
                }
            }
        }

        private async Task InitializeAfterConnectedAsync(CancellationToken cancellationToken)
        {
            try
            {
                await _dmChannels.InitializeAsync(_engine).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await _logger.WarningAsync("DM channel seed failed; continuing without DM cache.", ex).ConfigureAwait(false);
            }

            try
            {
                await SeedClanCacheAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await _logger.WarningAsync("Clan cache seed failed; continuing. Invite the bot to a clan and restart if commands never arrive.", ex).ConfigureAwait(false);
            }

            BindCacheListeners();
            await InitializeMmnAsync(cancellationToken).ConfigureAwait(false);
        }

        private async Task SeedClanCacheAsync(CancellationToken cancellationToken)
        {
            Exception? last = null;
            for (var attempt = 1; attempt <= 3; attempt++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    var options = new RequestOptions
                    {
                        SocketSendTimeout = Math.Max(Options.SocketTimeoutInMilliseconds, 30_000),
                    };
                    var list = await _engine.ListClanDescsAsync(new ListClanDescParams(), options).ConfigureAwait(false);
                    for (var i = 0; i < list.Clandesc.Count; i++)
                    {
                        var clanDesc = list.Clandesc[i].Proto;
                        Clans.Set(clanDesc.ClanId, new Clan(this, clanDesc));
                        MarkClanChatJoined(clanDesc.ClanId);
                        await _engine.JoinClanChatRtAsync(new ClanJoinParams(clanDesc.ClanId)).ConfigureAwait(false);
                    }

                    return;
                }
                catch (Exception ex) when (attempt < 3)
                {
                    last = ex;
                    await _logger.WarningAsync($"ListClanDescs attempt {attempt}/3 failed; retrying…", ex).ConfigureAwait(false);
                    await Task.Delay(TimeSpan.FromSeconds(attempt), cancellationToken).ConfigureAwait(false);
                }
            }

            if (last is not null)
            {
                throw last;
            }
        }

        public Task ConnectAgentSseAsync(CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(Options.AgentEventUrl))
            {
                throw new InvalidOperationException(
                    "AgentEventUrl is not configured. Set MezonClientOptions.AgentEventUrl to your agent SSE base URL.");
            }

            _agentManager ??= new AgentSseManager(Options.AgentEventUrl, Options.BotId, Options.Token);
            _agentManager.MessageReceived += async evt =>
            {
                switch (evt.EventType)
                {
                    case "room_started":
                        if (AgentSessionStartedInternal != null)
                        {
                            await AgentSessionStartedInternal(evt).ConfigureAwait(false);
                        }
                        break;
                    case "room_ended":
                        if (AgentSessionEndedInternal != null)
                        {
                            await AgentSessionEndedInternal(evt).ConfigureAwait(false);
                        }
                        break;
                    case "room_summary_done":
                        if (AgentSessionSummaryDoneInternal != null)
                        {
                            await AgentSessionSummaryDoneInternal(evt).ConfigureAwait(false);
                        }
                        break;
                }
            };
            return _agentManager.ConnectAsync(cancellationToken);
        }

        /// <summary>Re-list clans and JoinClanChat (safe to call after Ready if seed failed).</summary>
        public Task RefreshClanMembershipAsync(CancellationToken cancellationToken = default)
            => SeedClanCacheAsync(cancellationToken);

        public Task<UploadAttachmentResponse> UploadAttachmentFileAsync(UploadAttachmentParams body, RequestOptions? options = null)
            => _engine.UploadAttachmentFileAsync(body, options);

        public Task<MultipartUploadAttachmentResponse> MultipartUploadAttachmentFileStartAsync(
            UploadAttachmentParams body,
            RequestOptions? options = null)
            => _engine.MultipartUploadAttachmentFileStartAsync(body, options);

        public Task<UploadAttachmentResponse> MultipartUploadAttachmentFileFinishAsync(
            MultipartUploadAttachmentFinishParams body,
            RequestOptions? options = null)
            => _engine.MultipartUploadAttachmentFileFinishAsync(body, options);

        public Task<ChannelDescListResponse> ListChannelDescsAsync(ListChannelDescsParams request, RequestOptions? options = null)
            => _engine.ListChannelDescsAsync(request, options);

        public Task<ChannelDescriptionResponse> GetChannelDetailAsync(long channelId, RequestOptions? options = null)
            => _engine.GetChannelDetailAsync(channelId, options);

        public Task<ClanDescListResponse> ListClanDescsAsync(ListClanDescParams body, RequestOptions? options = null)
            => _engine.ListClanDescsAsync(body, options);

        public Task<ChannelDescriptionResponse> CreateChannelDescAsync(CreateChannelDescParams body, RequestOptions? options = null)
            => _engine.CreateChannelDescAsync(body, options);

        public Task<VoiceChannelUserListResponse> ListChannelVoiceUsersAsync(long clanId, long channelId, int channelType, RequestOptions? options = null)
            => _engine.ListChannelVoiceUsersAsync(clanId, channelId, channelType, options);

        public Task<StreamHttpCallbackResponse> StreamingServerCallbackAsync(StreamHttpCallbackParams body, RequestOptions? options = null)
            => _engine.StreamingServerCallbackAsync(body, options);

        public Task<RoleListEventResponse> ListRolesAsync(RoleListEventParams request, RequestOptions? options = null)
            => _engine.ListRolesAsync(request, options);

        public Task<RoleUserListResponse> ListRoleUsersAsync(ListRoleUsersParams request, RequestOptions? options = null)
            => _engine.ListRoleUsersAsync(request, options);

        public Task<RoleResponse> CreateRoleAsync(CreateRoleParams body, RequestOptions? options = null)
            => _engine.CreateRoleAsync(body, options);

        public Task UpdateRoleAsync(UpdateRoleParams body, RequestOptions? options = null)
            => _engine.UpdateRoleAsync(body, options);

        public Task<RoleListResponse> GetRoleOfUserInTheClanAsync(long clanId, RequestOptions? options = null)
            => _engine.GetRoleOfUserInTheClanAsync(clanId, options);

        public Task<FriendListResponse> ListFriendsAsync(int? state = null, int? limit = null, string? cursor = null, RequestOptions? options = null)
            => _engine.ListFriendsAsync(state, limit, cursor, options);

        public Task<AddFriendsResponse> AddFriendsAsync(IEnumerable<long>? ids = null, IEnumerable<string>? usernames = null, RequestOptions? options = null)
            => _engine.AddFriendsAsync(ids, usernames, options);

        public Task AddQuickMenuAccessAsync(QuickMenuAccessParams body, RequestOptions? options = null)
            => _engine.AddQuickMenuAccessAsync(body, options);

        public Task UpdateQuickMenuAccessAsync(QuickMenuAccessParams body, RequestOptions? options = null)
            => _engine.UpdateQuickMenuAccessAsync(body, options);

        public Task DeleteQuickMenuAccessAsync(QuickMenuAccessParams body, RequestOptions? options = null)
            => _engine.DeleteQuickMenuAccessAsync(body, options);

        public Task<GenerateMeetTokenResponse> GenerateMeetTokenAsync(GenerateMeetTokenParams body, RequestOptions? options = null)
            => _engine.GenerateMeetTokenAsync(body, options);

        public Task<ChannelMessageAckResponse> SendChannelMessageAsync(SendChannelMessageParams message, RequestOptions? options = null)
            => MessageSendHelper.SendAsync(_engine, message, options);

        public Task UpdateChannelMessageAsync(ChannelMessageUpdateParams body, RequestOptions? options = null)
            => _engine.UpdateChannelMessageAsync(body, options);

        public Task DeleteChannelMessageAsync(ChannelMessageRemoveParams body, RequestOptions? options = null)
            => _engine.DeleteChannelMessageAsync(body, options);

        public ValueTask<Clan> GetClanAsync(long clanId, CancellationToken cancellationToken = default)
            => Clans.GetOrFetchAsync(clanId, FetchClanAsync, cancellationToken);

        public ValueTask<Channel> GetChannelAsync(long channelId, CancellationToken cancellationToken = default)
            => Channels.GetOrFetchAsync(channelId, FetchChannelAsync, cancellationToken);

        /// <summary>
        ///     Returns a cached channel or inserts a lightweight stub without calling the socket API.
        ///     Used by interaction/command hot paths when the channel is not yet warmed in cache.
        /// </summary>
        internal Channel GetOrCreateChannelStub(long channelId, long clanId = 0)
        {
            if (Channels.TryGet(channelId, out var existing))
            {
                return existing;
            }

            if (!Clans.TryGet(clanId, out var clan))
            {
                clan = new Clan(this, new global::Mezon.Net.Internal.Api.ClanDesc { ClanId = clanId });
                Clans.Set(clanId, clan);
            }

            var channel = new Channel(
                this,
                new global::Mezon.Net.Internal.Api.ChannelDescription
                {
                    ChannelId = channelId,
                    ClanId = clanId,
                },
                clan);
            Channels.Set(channelId, channel);
            return channel;
        }

        public ValueTask<Entities.User> GetUserAsync(long userId, CancellationToken cancellationToken = default)
            => Users.GetOrFetchAsync(userId, FetchUserAsync, cancellationToken);

        private async ValueTask<Clan> FetchClanAsync(long clanId, CancellationToken cancellationToken)
        {
            var list = await _engine.ListClanDescsAsync(new ListClanDescParams()).ConfigureAwait(false);
            for (var i = 0; i < list.Clandesc.Count; i++)
            {
                var clan = list.Clandesc[i].Proto;
                if (clan.ClanId == clanId)
                {
                    return new Clan(this, clan);
                }
            }

            throw new MezonEntityNotFoundException(nameof(Clan), clanId);
        }

        private async ValueTask<Channel> FetchChannelAsync(long channelId, CancellationToken cancellationToken)
        {
            var detail = await _engine.GetChannelDetailAsync(channelId).ConfigureAwait(false);
            var clan = await GetClanAsync(detail.ClanId, cancellationToken).ConfigureAwait(false);
            return new Channel(this, detail.Proto, clan);
        }

        /// <summary>Upsert a channel into L1 from an API/event description (caller-initiated or event payload).</summary>
        internal Channel UpsertChannelFromDescription(global::Mezon.Net.Internal.Api.ChannelDescription desc, Clan? clan = null)
        {
            if (desc.ChannelId == 0)
            {
                throw new System.ArgumentException("ChannelDescription.ChannelId is required.", nameof(desc));
            }

            if (clan is null)
            {
                var clanId = desc.ClanId;
                if (!Clans.TryGet(clanId, out clan))
                {
                    clan = new Clan(this, new global::Mezon.Net.Internal.Api.ClanDesc { ClanId = clanId });
                    Clans.Set(clanId, clan);
                }
            }

            if (Channels.TryGet(desc.ChannelId, out var existing))
            {
                existing.UpdateFrom(desc);
                return existing;
            }

            var channel = new Channel(this, desc, clan);
            Channels.Set(desc.ChannelId, channel);
            return channel;
        }

        private ValueTask<Entities.User> FetchUserAsync(long userId, CancellationToken cancellationToken)
        {
            _dmChannels.TryGetDmChannelId(userId, out var dmChannelId);
            return new ValueTask<Entities.User>(new Entities.User(this, userId, dmChannelId: dmChannelId));
        }

        partial void DisposeMmn();

        /// <summary>
        ///     Sends a channel message without entering the transport rate limiter or channel send queue.
        ///     Wired onto <see cref="IRateLimitInfo.SendBypassMessageAsync"/> for rate-limit warning callbacks.
        ///     <paramref name="text"/> may be plain text or full message-content JSON (e.g. an embed payload).
        /// </summary>
        private Task SendRateLimitBypassMessageAsync(long clanId, long channelId, string text)
        {
            var isPublic = true;
            var channelType = (int)ChannelType.Channel;
            if (Channels.TryGet(channelId, out var channel))
            {
                clanId = channel.ClanId;
                isPublic = channel.IsPublic;
                channelType = channel.Type;
            }

            var contentJson = LooksLikeMessageContentJson(text)
                ? MessageContent.Parse(text).ToJson()
                : MessageContent.CreateText(text).ToJson();

            var parameters = new SendChannelMessageParams(
                clanId,
                channelId,
                contentJson,
                isPublic: isPublic,
                mode: ChannelModeConverter.ToStreamMode(channelType));

            var options = new RequestOptions { BypassRateLimiter = true };
            return MessageSendHelper.SendAsync(_engine, parameters, options);
        }

        private static bool LooksLikeMessageContentJson(string text)
        {
            if (string.IsNullOrWhiteSpace(text))
            {
                return false;
            }

            var trimmed = text.AsSpan().TrimStart();
            return trimmed.Length > 0 && trimmed[0] == '{';
        }

        public async ValueTask DisposeAsync()
        {
            _agentManager?.Dispose();
            DisposeMmn();
            try
            {
                if (_engine.ConnectionState != ConnectionState.Disconnected)
                {
                    await _engine.DisconnectAsync().ConfigureAwait(false);
                }
            }
            catch
            {
                // Best-effort disconnect before dispose.
            }

            await _engine.DisposeAsync().ConfigureAwait(false);
            _initializeGate.Dispose();
            Clans.Clear();
            Channels.Clear();
            Users.Clear();
        }
    }
}
