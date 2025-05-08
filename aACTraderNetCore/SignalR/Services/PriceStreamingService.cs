using aACTrader.SignalR.Hubs;
using LazyCache;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;

namespace aACTrader.SignalR.Services
{
    public class PriceStreamingService
    {
        private readonly ILogger<PriceStreamingService> _logger;
        private readonly IHubContext<PriceStreamingHub> _hubContext;
        private readonly CachingService _cache;

        public PriceStreamingService(ILogger<PriceStreamingService> logger, IHubContext<PriceStreamingHub> hubContext, CachingService cache)
        {
            _logger = logger;
            _hubContext = hubContext;
            _cache = cache;
        }
    }
}