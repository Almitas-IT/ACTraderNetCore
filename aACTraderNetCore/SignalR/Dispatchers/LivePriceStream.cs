using aACTrader.Samples;
using aACTrader.SignalR.Hubs;
using aCommons;
using LazyCache;
using Microsoft.AspNetCore.SignalR;
using System;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;

namespace aACTrader.SignalR.Dispatchers
{
    public class LivePriceStream
    {
        private CachingService _cache { get; set; }

        private readonly SemaphoreSlim _marketStateLock = new SemaphoreSlim(1, 1);
        private readonly Subject<SecurityPrice> _subject = new Subject<SecurityPrice>();

        private volatile MarketState _marketState;

        public LivePriceStream(IHubContext<StockPriceHub> hub, CachingService cache)
        {
            Hub = hub;
            _cache = cache;
        }

        private IHubContext<StockPriceHub> Hub
        {
            get;
            set;
        }

        public MarketState MarketState
        {
            get { return _marketState; }
            private set { _marketState = value; }
        }

        public IObservable<SecurityPrice> StreamStocks()
        {
            return _subject;
        }

        public async Task OpenMarket()
        {
            await _marketStateLock.WaitAsync();
            try
            {
                if (MarketState != MarketState.Open)
                {
                    MarketState = MarketState.Open;
                    await BroadcastMarketStateChange(MarketState.Open);
                }
            }
            finally
            {
                _marketStateLock.Release();
            }
        }

        public async Task CloseMarket()
        {
            await _marketStateLock.WaitAsync();
            try
            {
                if (MarketState == MarketState.Open)
                {
                    MarketState = MarketState.Closed;
                    await BroadcastMarketStateChange(MarketState.Closed);
                }
            }
            finally
            {
                _marketStateLock.Release();
            }
        }

        public void Publish(SecurityPrice securityPrice)
        {
            _subject.OnNext(securityPrice);
        }

        private async Task BroadcastMarketStateChange(MarketState marketState)
        {
            switch (marketState)
            {
                case MarketState.Open:
                    await Hub.Clients.All.SendAsync("marketOpened");
                    break;
                case MarketState.Closed:
                    await Hub.Clients.All.SendAsync("marketClosed");
                    break;
                default:
                    break;
            }
        }

        private async Task BroadcastMarketReset()
        {
            await Hub.Clients.All.SendAsync("marketReset");
        }
    }
}
