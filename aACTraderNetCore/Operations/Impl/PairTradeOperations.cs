using aCommons;
using aCommons.Cef;
using aCommons.Trading;
using aCommons.Utils;
using LazyCache;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Configuration;

namespace aACTrader.Operations.Impl
{
    public class PairTradeOperations
    {
        private readonly ILogger<PairTradeOperations> _logger;
        private readonly CachingService _cache;

        protected static readonly string ENV = ConfigurationManager.AppSettings["ENV"];

        public PairTradeOperations(ILogger<PairTradeOperations> logger, CachingService cache)
        {
            _logger = logger;
            _cache = cache;
        }

        public void ProcessNewOrders(IList<PairOrder> orderList)
        {
            IDictionary<string, OrderSummary> orderSummaryDict;
            string environment = "PRODUCTION";

            if (ENV.Equals("PROD"))
            {
                orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_SUMMARY);
            }
            else
            {
                orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.SIM_ORDER_SUMMARY);
                environment = "SIMULATION";
            }

            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
        }

        public void ProcessPairTrades()
        {
            IDictionary<string, OrderSummary> orderSummaryDict;
            string environment = "PRODUCTION";

            if (ENV.Equals("PROD"))
            {
                orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_SUMMARY);
            }
            else
            {
                orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.SIM_ORDER_SUMMARY);
                environment = "SIMULATION";
            }

            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);

            //List of Orders to auto-update
            IList<NewOrder> updateOrders = new List<NewOrder>();

            //Process Pair Trades
            IDictionary<string, NewPairOrder> pairTradeDict = _cache.Get<IDictionary<string, NewPairOrder>>(CacheKeys.PAIR_TRADE_MAP);
            foreach (KeyValuePair<string, NewPairOrder> kvp in pairTradeDict)
            {
                string parentId = kvp.Key;
                NewPairOrder pairTrade = kvp.Value;

                double pairRatio = pairTrade.PairRatio.GetValueOrDefault();
                double buyAggresiveRatio = 0;
                double sellAggresiveRatio = 0;
                double buySellAggresiveRatio = 0;

                OrderSummary buyOrder;
                OrderSummary sellOrder;

                if (orderSummaryDict.TryGetValue(pairTrade.BuyOrderId, out buyOrder) &&
                    orderSummaryDict.TryGetValue(pairTrade.SellOrderId, out sellOrder))
                {
                    string buySecuritySymbol = !string.IsNullOrEmpty(buyOrder.ALMSym) ? buyOrder.ALMSym : buyOrder.BBGSym;
                    SecurityPrice buySecurityPrice = SecurityPriceLookupOperations.GetSecurityPrice(buySecuritySymbol, priceTickerMap, securityPriceDict);

                    string sellSecuritySymbol = !string.IsNullOrEmpty(sellOrder.ALMSym) ? sellOrder.ALMSym : sellOrder.BBGSym;
                    SecurityPrice sellSecurityPrice = SecurityPriceLookupOperations.GetSecurityPrice(sellSecuritySymbol, priceTickerMap, securityPriceDict);

                    //Calculate Buy Ratio (Sell @ LastPrice/Buy @ AskPrice) -- Buy Aggresive Policy
                    if (sellSecurityPrice.LastPrc.HasValue && buySecurityPrice.AskPrc.HasValue)
                        buyAggresiveRatio = sellSecurityPrice.LastPrc.GetValueOrDefault() / buySecurityPrice.AskPrc.GetValueOrDefault();

                    //Calculate Sell Ratio (Sell @ BidPrice/Buy @ LastPrice) -- Sell Aggresive Policy
                    if (sellSecurityPrice.BidPrc.HasValue && buySecurityPrice.LastPrc.HasValue)
                        sellAggresiveRatio = sellSecurityPrice.BidPrc.GetValueOrDefault() / buySecurityPrice.LastPrc.GetValueOrDefault();

                    //Calculate Buy Sell Ratio (Sell @ LastPrice/Buy @ LastPrice) -- Buy Sell Aggresive Policy
                    if (sellSecurityPrice.LastPrc.HasValue && buySecurityPrice.LastPrc.HasValue)
                        buySellAggresiveRatio = sellSecurityPrice.LastPrc.GetValueOrDefault() / buySecurityPrice.LastPrc.GetValueOrDefault();

                    //Check all 3 ratios against Pair Ratio
                    if (pairTrade.PairRatio.HasValue)
                    {
                        bool isRatioInRange = false;
                        if (buyAggresiveRatio > pairRatio &&
                            sellAggresiveRatio > pairRatio &&
                            buySellAggresiveRatio > pairRatio)
                            isRatioInRange = true;

                        if (isRatioInRange)
                        {
                            double minRatio = CommonUtils.Min(new double[] { buyAggresiveRatio, sellAggresiveRatio, buySellAggresiveRatio });

                            if (minRatio == buyAggresiveRatio)
                            {
                                //buyOrder.RefIndexTargetPrice =  
                            }
                        }
                    }
                }
            }
        }
    }
}