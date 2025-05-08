using aACTrader.Services.Messaging;
using Microsoft.Extensions.Logging;

namespace aACTrader.Services
{
    public class NeovestSharesImbalanceService
    {
        private readonly ILogger<NeovestSharesImbalanceService> _logger;
        private NeovestSharesImbalanceConsumer _consumer { get; set; }

        public NeovestSharesImbalanceService(ILogger<NeovestSharesImbalanceService> logger, NeovestSharesImbalanceConsumer consumer)
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