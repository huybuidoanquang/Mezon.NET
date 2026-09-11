using System;
using System.Reflection;
using System.Threading.Tasks;
using Mezon.Net.Abstractions;
using Mezon.Net.Sdk;
using Xunit;

namespace Mezon.Net.Sdk.Tests
{
    public sealed class SessionCredentialTests
    {
        [Fact]
        public async Task GetSessionIdAsyncReturnsCurrentSessionId()
        {
            await using var client = new MezonClient(new MezonClientOptions(1, "bot-token"));
            var engineType = client.Engine.GetType();
            FieldInfo? sessionManagerField = null;
            for (var type = engineType; type is not null && sessionManagerField is null; type = type.BaseType)
            {
                sessionManagerField = type.GetField("_sessionManager", BindingFlags.Instance | BindingFlags.NonPublic);
            }
            Assert.NotNull(sessionManagerField);
            var sessionManager = sessionManagerField.GetValue(client.Engine)!;
            sessionManager.GetType()
                .GetField("_session", BindingFlags.Instance | BindingFlags.NonPublic)!
                .SetValue(sessionManager, new TestSession("current-sid"));

            Assert.Equal("current-sid", await client.GetSessionIdAsync());
        }

        private sealed class TestSession : ISession
        {
            public TestSession(string sessionId) => SessionId = sessionId;

            public string SessionId { get; }
            public string AuthToken => "valid-auth-token";
            public string RefreshToken => "valid-refresh-token";
            public bool Created => true;
            public long CreatedAt => DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            public long ExpiresAt => DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeSeconds();
            public long RefreshExpiresAt => DateTimeOffset.UtcNow.AddDays(1).ToUnixTimeSeconds();
            public string? Username => "test";
            public string? UserId => "1";
            public bool IsRemember => false;
            public string? ApiUrl => null;
            public string? WsUrl => null;
            public string? TcpUrl => null;
            public string? IdToken => null;
            public bool IsExpiredSoon(int seconds) => false;
            public bool IsRefreshExpiredSoon(int seconds) => false;
            public bool IsExpired() => false;
            public bool IsRefreshExpired() => false;
        }
    }
}
