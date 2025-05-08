using aACTrader.Samples;
using aACTrader.SignalR.Dispatchers;
using aCommons;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace aACTrader.SignalR.Hubs
{
    public class LivePriceStreamHub : Hub
    {
        private readonly ILogger<LivePriceStreamHub> _logger;
        private readonly LivePriceStream _livePriceStream;

        public LivePriceStreamHub(ILogger<LivePriceStreamHub> logger, LivePriceStream livePriceStream)
        {
            _logger = logger;
            _livePriceStream = livePriceStream;
        }

        public ChannelReader<SecurityPrice> StreamStocks()
        {
            return _livePriceStream.StreamStocks().AsChannelReader(256000);
        }
    }
}
