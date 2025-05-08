using aACTrader.Services.Messaging;
using Microsoft.Extensions.Logging;

namespace aACTrader.Services
{
    public class NeovestFundPricingService
    {
        private readonly ILogger<NeovestFundPricingService> _logger;
        private NeovestFundPricingConsumer _consumer { get; set; }

        public NeovestFundPricingService(ILogger<NeovestFundPricingService> logger, NeovestFundPricingConsumer consumer)
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