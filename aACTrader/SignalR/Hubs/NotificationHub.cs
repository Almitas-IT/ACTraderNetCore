using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace aACTrader.SignalR.Hubs
{
    public class NotificationHub : Hub
    {
        private readonly ILogger<NotificationHub> _logger;
        private readonly static ConnectionMapping<string> _connections = new ConnectionMapping<string>();

        public NotificationHub(ILogger<NotificationHub> logger)
        {
            _logger = logger;
        }

        public override Task OnConnectedAsync()
        {
            string name = Context.User.Identity.Name;
            if (string.IsNullOrEmpty(name))
                name = Context.ConnectionId;
            _connections.Add(name, Context.ConnectionId);
            _logger.LogInformation("[NotificationHub] User Connected: " + name + ", # of Users " + _connections.Count);
            return base.OnConnectedAsync();
        }

        public override Task OnDisconnectedAsync(Exception? exception)
        {
            string name = Context.User.Identity.Name;
            if (string.IsNullOrEmpty(name))
                name = Context.ConnectionId;
            _connections.Remove(name, Context.ConnectionId);
            _logger.LogInformation("[NotificationHub] User Disconnected: " + name + ", # of Users " + _connections.Count);
            return base.OnDisconnectedAsync(exception);
        }

        //public async Task PositionUpdate()
        //{
        //    _logger.LogInformation("Sending Position Update message...");
        //    await Clients.All.SendAsync("PositionUpdate");
        //}
    }
}
