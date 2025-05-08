using aACTrader.Services.EMSX;
using Microsoft.Extensions.Logging;

namespace aACTrader.Services
{
    public class EMSXOrderErrorService
    {
        private readonly ILogger<EMSXOrderErrorService> _logger;
        private EMSXOrderErrorConsumer _consumer { get; set; }

        public EMSXOrderErrorService(ILogger<EMSXOrderErrorService> logger, EMSXOrderErrorConsumer consumer)
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