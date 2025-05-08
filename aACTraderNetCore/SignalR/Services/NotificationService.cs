using aACTrader.SignalR.Hubs;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace aACTrader.SignalR.Services
{
    public class NotificationService
    {
        private readonly ILogger<NotificationService> _logger;
        private readonly IHubContext<NotificationHub> _hubContext;

        public NotificationService(ILogger<NotificationService> logger, IHubContext<NotificationHub> hubContext)
        {
            _logger = logger;
            _hubContext = hubContext;
        }

        public async Task PositionUpdate()
        {
            _logger.LogInformation("Sending Position Update message...");
            await _hubContext.Clients.All.SendAsync("PositionUpdate");
        }

        public async Task FundSupplementalDataUpdate()
        {
            _logger.LogInformation("Sending Fund Supplemental Data Update message...");
            await _hubContext.Clients.All.SendAsync("FundSupplementalDataUpdate");
        }

        public async Task SecurityMstExtUpdate()
        {
            _logger.LogInformation("Sending Security Master Extension Update message...");
            await _hubContext.Clients.All.SendAsync("SecurityMstExtUpdate");
        }
    }
}
