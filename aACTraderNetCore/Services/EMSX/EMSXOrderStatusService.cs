using aACTrader.Services.EMSX;
using Microsoft.Extensions.Logging;

namespace aACTrader.Services
{
    public class EMSXOrderStatusService
    {
        private readonly ILogger<EMSXOrderStatusService> _logger;
        private EMSXOrderStatusConsumer _consumer { get; set; }

        public EMSXOrderStatusService(ILogger<EMSXOrderStatusService> logger, EMSXOrderStatusConsumer consumer)
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