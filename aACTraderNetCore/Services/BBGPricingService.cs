using aACTrader.Services.Messaging;
using Microsoft.Extensions.Logging;

namespace aACTrader.Services
{
    public class BBGPricingService
    {
        private readonly ILogger<BBGPricingService> _logger;
        private BBGPricingConsumer _consumer { get; set; }

        public BBGPricingService(ILogger<BBGPricingService> logger, BBGPricingConsumer consumer)
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