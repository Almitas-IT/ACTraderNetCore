using aACTrader.Services.Messaging;
using Microsoft.Extensions.Logging;

namespace aACTrader.Services
{
    public class SimNeovestOrderSummaryService
    {
        private readonly ILogger<SimNeovestOrderSummaryService> _logger;
        private SimNeovestOrderSummaryConsumer _consumer { get; set; }

        public SimNeovestOrderSummaryService(ILogger<SimNeovestOrderSummaryService> logger, SimNeovestOrderSummaryConsumer consumer)
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