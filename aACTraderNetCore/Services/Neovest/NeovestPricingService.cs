using aACTrader.Services.Messaging;
using Microsoft.Extensions.Logging;

namespace aACTrader.Services
{
    public class NeovestPricingService
    {
        private readonly ILogger<NeovestPricingService> _logger;
        private NeovestPricingConsumer _consumer { get; set; }

        public NeovestPricingService(ILogger<NeovestPricingService> logger, NeovestPricingConsumer consumer)
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