using Microsoft.Extensions.Logging;

namespace aACTrader.Services.Crypto
{
    public class CoinAPIQuoteService
    {
        private readonly ILogger<CoinAPIQuoteConsumer> _logger;
        private CoinAPIQuoteConsumer _consumer { get; set; }

        public CoinAPIQuoteService(ILogger<CoinAPIQuoteConsumer> logger, CoinAPIQuoteConsumer consumer)
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
