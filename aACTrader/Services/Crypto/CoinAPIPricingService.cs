using aACTrader.Services.Messaging;
using Microsoft.Extensions.Logging;

namespace aACTrader.Services
{
    public class CoinAPIPricingService
    {
        private readonly ILogger<CoinAPIPricingConsumer> _logger;
        private CoinAPIPricingConsumer _consumer { get; set; }

        public CoinAPIPricingService(ILogger<CoinAPIPricingConsumer> logger, CoinAPIPricingConsumer consumer)
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