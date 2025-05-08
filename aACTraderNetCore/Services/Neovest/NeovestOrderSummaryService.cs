using aACTrader.Services.Messaging;
using Microsoft.Extensions.Logging;

namespace aACTrader.Services
{
    public class NeovestOrderSummaryService
    {
        private readonly ILogger<NeovestOrderSummaryService> _logger;
        private NeovestOrderSummaryConsumer _consumer { get; set; }

        public NeovestOrderSummaryService(ILogger<NeovestOrderSummaryService> logger, NeovestOrderSummaryConsumer consumer)
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