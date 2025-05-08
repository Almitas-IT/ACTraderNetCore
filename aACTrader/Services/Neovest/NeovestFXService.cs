using aACTrader.Services.Messaging;
using Microsoft.Extensions.Logging;

namespace aACTrader.Services
{
    public class NeovestFXService
    {
        private readonly ILogger<NeovestFXService> _logger;
        private NeovestFXConsumer _consumer { get; set; }

        public NeovestFXService(ILogger<NeovestFXService> logger, NeovestFXConsumer consumer)
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