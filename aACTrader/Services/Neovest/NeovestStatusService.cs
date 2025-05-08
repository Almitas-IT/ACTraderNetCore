using aACTrader.Services.Messaging;
using Microsoft.Extensions.Logging;

namespace aACTrader.Services
{
    public class NeovestStatusService
    {
        private readonly ILogger<NeovestStatusService> _logger;
        private NeovestStatusConsumer _consumer { get; set; }

        public NeovestStatusService(ILogger<NeovestStatusService> logger, NeovestStatusConsumer consumer)
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