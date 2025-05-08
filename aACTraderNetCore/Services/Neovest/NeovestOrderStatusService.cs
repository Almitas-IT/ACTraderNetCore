using aACTrader.Services.Messaging;
using Microsoft.Extensions.Logging;

namespace aACTrader.Services
{
    public class NeovestOrderStatusService
    {
        private readonly ILogger<NeovestOrderStatusService> _logger;
        private NeovestOrderStatusConsumer _consumer { get; set; }

        public NeovestOrderStatusService(ILogger<NeovestOrderStatusService> logger)
        {
            _logger = logger;
        }

        public void Start()
        {
            _consumer.Initialize();
            _consumer.Start();
        }
    }
}