using Microsoft.Extensions.Logging;

namespace aACTrader.Services.EMSX
{
    public class EMSXRouteStatusService
    {
        private readonly ILogger<EMSXRouteStatusService> _logger;
        private EMSXRouteStatusConsumer _consumer { get; set; }

        public EMSXRouteStatusService(ILogger<EMSXRouteStatusService> logger, EMSXRouteStatusConsumer consumer)
        {
            _logger = logger;
            _consumer = consumer;
        }

        public void Start()
        {
            _consumer.Initialize();
            _consumer.Start();
        }
    }
}
