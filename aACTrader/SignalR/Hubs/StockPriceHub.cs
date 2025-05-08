using aACTrader.Samples;
using aCommons;
using Microsoft.AspNetCore.SignalR;
using System.Collections.Generic;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace aACTrader.SignalR.Hubs
{
    public class StockPriceHub : Hub
    {
        private readonly StockTicker _stockTicker;

        public StockPriceHub(StockTicker stockTicker)
        {
            _stockTicker = stockTicker;
        }

        public IEnumerable<Stock> GetAllStocks()
        {
            return _stockTicker.GetAllStocks();
        }

        public ChannelReader<SecurityPrice> StreamStocks()
        {
            return _stockTicker.StreamStocks().AsChannelReader(256000);
        }

        public string GetMarketState()
        {
            return _stockTicker.MarketState.ToString();
        }

        public async Task OpenMarket()
        {
            await _stockTicker.OpenMarket();
        }

        public async Task CloseMarket()
        {
            await _stockTicker.CloseMarket();
        }

        public async Task Reset()
        {
            await _stockTicker.Reset();
        }
    }
}
