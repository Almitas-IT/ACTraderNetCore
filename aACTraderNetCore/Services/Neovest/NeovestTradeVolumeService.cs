using aACTrader.Services.Messaging;
using Microsoft.Extensions.Logging;

namespace aACTrader.Services
{
    public class NeovestTradeVolumeService
    {
        private readonly ILogger<NeovestTradeVolumeService> _logger;
        private NeovestTradeVolumeConsumer _consumer { get; set; }

        public NeovestTradeVolumeService(ILogger<NeovestTradeVolumeService> logger, NeovestTradeVolumeConsumer consumer)
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