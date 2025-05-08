using aACTrader.DAO.Repository;
using aACTrader.Operations.Impl;
using aACTrader.Services.Crypto;
using aACTrader.Services.Messaging;
using aCommons;
using aCommons.DTO;
using aCommons.Security;
using aCommons.Trading;
using LazyCache;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Services
{
    public class PricingService
    {
        private readonly ILogger<PricingService> _logger;

        private BBGPricingService _bbgPricingService { get; set; }
        private BBGDataUpdateConsumer _bbgDataUpdateConsumer { get; set; }
        private NeovestStatusService _neovestStatusService { get; set; }
        private NeovestFundPricingService _neovestFundPricingService { get; set; }
        private NeovestPricingService _neovestPricingService { get; set; }
        private NeovestFXService _neovestFXService { get; set; }
        private NeovestOrderSummaryService _neovestOrderSummaryService { get; set; }
        private SimNeovestOrderSummaryService _simNeovestOrderSummaryService { get; set; }
        private NeovestSharesImbalanceService _neovestSharesImbalanceService { get; set; }
        private NeovestTradeVolumeService _neovestTradeVolumeService { get; set; }
        private CoinAPIPricingService _coinAPIPricingService { get; set; }
        private CoinAPIQuoteService _coinAPIQuoteService { get; set; }
        private CachingService _cache { get; set; }
        private SecurityPriceOperation _securityPriceOperation { get; set; }
        private CommonDao _commonDao { get; set; }
        private TradingDao _tradingDao { get; set; }
        private PairTradingDao _pairTradingDao { get; set; }
        private PairTradingNVDao _pairTradingNVDao { get; set; }
        private readonly IConfiguration _configuration;

        public PricingService(ILogger<PricingService> logger
            , BBGPricingService bbgPricingService
            , NeovestStatusService neovestStatusService
            , NeovestFundPricingService neovestFundPricingService
            , NeovestPricingService neovestPricingService
            , NeovestFXService neovestFXService
            , NeovestOrderSummaryService neovestOrderSummaryService
            , SimNeovestOrderSummaryService simNeovestOrderSummaryService
            , CoinAPIPricingService coinAPIPricingService
            , CoinAPIQuoteService coinAPIQuoteService
            , CachingService cache
            , SecurityPriceOperation securityPriceOperation
            , CommonDao commonDao
            , TradingDao tradingDao
            , PairTradingDao pairTradingDao
            , PairTradingNVDao pairTradingNVDao
            , BBGDataUpdateConsumer bbgDataUpdateConsumer
            , IConfiguration configuration
            , NeovestSharesImbalanceService neovestSharesImbalanceService
            , NeovestTradeVolumeService neovestTradeVolumeService)
        {
            _logger = logger;
            _bbgPricingService = bbgPricingService;
            _neovestStatusService = neovestStatusService;
            _neovestFundPricingService = neovestFundPricingService;
            _neovestPricingService = neovestPricingService;
            _neovestFXService = neovestFXService;
            _neovestOrderSummaryService = neovestOrderSummaryService;
            _simNeovestOrderSummaryService = simNeovestOrderSummaryService;
            _coinAPIPricingService = coinAPIPricingService;
            _coinAPIQuoteService = coinAPIQuoteService;
            _cache = cache;
            _securityPriceOperation = securityPriceOperation;
            _commonDao = commonDao;
            _tradingDao = tradingDao;
            _pairTradingDao = pairTradingDao;
            _pairTradingNVDao = pairTradingNVDao;
            _bbgDataUpdateConsumer = bbgDataUpdateConsumer;
            _configuration = configuration;
            _neovestSharesImbalanceService = neovestSharesImbalanceService;
            _neovestTradeVolumeService = neovestTradeVolumeService;
        }

        public void Start()
        {
            _logger.LogInformation("Starting Pricing Service(s)...");

            _cache.Add(CacheKeys.SECURITY_PRICES, new Dictionary<string, SecurityPrice>(StringComparer.CurrentCultureIgnoreCase), DateTimeOffset.MaxValue);

            _logger.LogInformation("Populating Pricing Cache with existing prices...");

            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);

            //populate Neovest and BBG prices
            IList<SecurityPrice> securityPrices = _securityPriceOperation.GetLatestPrices();
            foreach (SecurityPrice price in securityPrices)
            {
                //if (price.Ticker.Contains("BCECN V3"))
                //    _logger.LogInformation("Ticker.... " + price.Ticker);

                if (!securityPriceDict.ContainsKey(price.Ticker))
                    securityPriceDict.Add(price.Ticker, price);
            }

            //populate Broker prices
            IDictionary<string, Holding> holdingsDict = _cache.Get<IDictionary<string, Holding>>(CacheKeys.ALM_HOLDINGS_BY_PORT);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);

            foreach (KeyValuePair<string, Holding> kvp in holdingsDict)
            {
                Holding holding = kvp.Value;
                string ticker = holding.HoldingTicker;

                if (!string.IsNullOrEmpty(ticker) && !ticker.StartsWith("Swap"))
                {
                    //add security price if missing
                    SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(ticker, priceTickerMap, securityPriceDict);
                    if (securityPrice == null)
                    {
                        securityPrice = new SecurityPrice
                        {
                            Ticker = ticker,
                            LastPrc = holding.Price,
                        };

                        string source = string.Empty;
                        if (holding.Portfolio.IndexOf("Jeff", StringComparison.CurrentCultureIgnoreCase) >= 0)
                            source = "Jeff";
                        else if (holding.Portfolio.IndexOf("JPM", StringComparison.CurrentCultureIgnoreCase) >= 0)
                            source = "JPM";
                        else if (holding.Portfolio.IndexOf("IB", StringComparison.CurrentCultureIgnoreCase) >= 0)
                            source = "IB";
                        else if (holding.Portfolio.IndexOf("Fido", StringComparison.CurrentCultureIgnoreCase) >= 0)
                            source = "Fido";
                        securityPrice.Src = source;
                        securityPrice.RTFlag = 1;
                        securityPrice.MktCls = 0; //0 - Open, 1 - Closed

                        if (!securityPriceDict.ContainsKey(ticker))
                            securityPriceDict.Add(ticker, securityPrice);

                        _logger.LogInformation("Populating broker price for ticker: " + ticker);
                    }
                }
                else
                {
                    _logger.LogInformation("Ignoring Ticker.... " + ticker);
                }
            }

            _logger.LogInformation("Populating Dividends...");
            IDictionary<string, Dividend> Dividends = _commonDao.GetDividends();
            foreach (KeyValuePair<string, Dividend> kvp in Dividends)
            {
                string ticker = kvp.Key;
                Dividend dividend = kvp.Value;

                string pricingTicker = ticker;
                priceTickerMap.TryGetValue(ticker, out pricingTicker);

                if (!string.IsNullOrEmpty(pricingTicker)
                    && securityPriceDict.TryGetValue(pricingTicker, out SecurityPrice securityPrice))
                    securityPrice.DvdAmt = dividend.DividendAmount;
            }

            _logger.LogInformation("Populated Pricing Cache with existing prices...");

            _logger.LogInformation("Populated FX Rate Cache with existing (Neovest) FX Rates...");
            IDictionary<string, FXRate> fxRateDict = _securityPriceOperation.GetLatestFXRates();
            _cache.Add(CacheKeys.FX_RATES_LIVE, fxRateDict, DateTimeOffset.MaxValue);

            _logger.LogInformation("Populated Shares Imbalance Details...");
            IDictionary<string, SharesImbalance> sharesImbalanceDict = _securityPriceOperation.GetSharesImbalanceList();
            _cache.Add(CacheKeys.SHARES_IMBALANCE, sharesImbalanceDict, DateTimeOffset.MaxValue);

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            /// TRADING
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            _logger.LogInformation("Populated Order Details...");
            IDictionary<string, OrderSummary> orderSummaryDict = _tradingDao.GetOrderSummary();
            //PopulateRefIndex(orderSummaryDict);
            _cache.Add(CacheKeys.ORDER_SUMMARY, orderSummaryDict, DateTimeOffset.MaxValue);

            _logger.LogInformation("Populated Order Execution Details...");
            IDictionary<string, OrderSummary> orderExecutionDetails;
            if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                orderExecutionDetails = new Dictionary<string, OrderSummary>(StringComparer.CurrentCultureIgnoreCase);
            else
                orderExecutionDetails = _tradingDao.GetTradeExecutionDetails();
            _cache.Add(CacheKeys.ORDER_EXECUTION_DETAILS, orderExecutionDetails, DateTimeOffset.MaxValue);

            _logger.LogInformation("Populated Main Order Details...");
            IDictionary<string, MainOrderSummary> mainOrderSummaryDict = new Dictionary<string, MainOrderSummary>(StringComparer.CurrentCultureIgnoreCase);
            PopulateLatestOrders(mainOrderSummaryDict, orderSummaryDict, orderExecutionDetails);
            _cache.Add(CacheKeys.MAIN_ORDER_SUMMARY, mainOrderSummaryDict, DateTimeOffset.MaxValue);

            _logger.LogInformation("Populated Live Position/Trade Report Details...");
            IDictionary<string, IDictionary<string, PositionDetailTO>> tradeDetails = new Dictionary<string, IDictionary<string, PositionDetailTO>>();
            _cache.Add(CacheKeys.LIVE_TRADE_DETAILS, tradeDetails, DateTimeOffset.MaxValue);

            _logger.LogInformation("Populated Order Execution Summary...");
            IDictionary<string, TradePosition> tradePositionsDict = new Dictionary<string, TradePosition>(StringComparer.CurrentCultureIgnoreCase);
            _cache.Remove(CacheKeys.TRADE_EXECUTIONS);
            _cache.Add(CacheKeys.TRADE_EXECUTIONS, tradePositionsDict, DateTimeOffset.MaxValue);

            _logger.LogInformation("Populated Neovest Status Queue...");
            IList<ServiceStatus> list = new List<ServiceStatus>();
            _cache.Remove(CacheKeys.NEOVEST_STATUS);
            _cache.Add(CacheKeys.NEOVEST_STATUS, list, DateTimeOffset.MaxValue);

            _logger.LogInformation("Populated Batch Order Details...");
            PopulateBatchOrders();

            _logger.LogInformation("Populated Pair Order Details...");
            PopulatePairOrders();

            _logger.LogInformation("Populated Order Details by Symbol...");
            PopulateOrderDetailsBySymbol();

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            /// ORDER QUEUE
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            _logger.LogInformation("Populated Order Queue...");
            IDictionary<string, OrderSummary> orderQueueDict = _tradingDao.GetOrderQueue();
            _cache.Add(CacheKeys.ORDER_QUEUE_SUMMARY, orderQueueDict, DateTimeOffset.MaxValue);

            ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // SIMULATION/TEST
            ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            _logger.LogInformation("Populated Order Details [SIMULATED]...");
            IDictionary<string, OrderSummary> simOrderSummaryDict = _tradingDao.GetSimOrderSummary();
            _cache.Add(CacheKeys.SIM_ORDER_SUMMARY, simOrderSummaryDict, DateTimeOffset.MaxValue);

            _logger.LogInformation("Populated Order Execution Details [SIMULATED]...");
            IDictionary<string, OrderSummary> simOrderExecutionSummaryDict = _tradingDao.GetSimTradeExecutionDetails();
            _cache.Add(CacheKeys.SIM_ORDER_EXECUTION_DETAILS, simOrderExecutionSummaryDict, DateTimeOffset.MaxValue);

            _logger.LogInformation("Populated Order Queue...");
            IDictionary<string, OrderSummary> simOrderQueueDict = _tradingDao.GetOrderQueue();
            _cache.Add(CacheKeys.SIM_ORDER_QUEUE_SUMMARY, simOrderQueueDict, DateTimeOffset.MaxValue);

            _logger.LogInformation("Populated Batch Order Details [SIMULATED]...");
            PopulateSimBatchOrders();

            _logger.LogInformation("Populated Order Details by Symbol [SIMULATED]...");
            PopulateSimulatedOrderDetailsBySymbol();

            if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
            {
                _logger.LogInformation("Starting BBGUpdateConsumerService...");
                _bbgDataUpdateConsumer.Start();

                _logger.LogInformation("Starting BBGPricingService...");
                _bbgPricingService.Start();

                _logger.LogInformation("Starting NeovestStatusService...");
                _neovestStatusService.Start();

                _logger.LogInformation("Starting NeovestFundPricingService...");
                _neovestFundPricingService.Start();

                _logger.LogInformation("Starting NeovestPricingService...");
                _neovestPricingService.Start();

                _logger.LogInformation("Starting NeovestFXService...");
                _neovestFXService.Start();

                _logger.LogInformation("Starting NeovestOrderSummaryService...");
                _neovestOrderSummaryService.Start();

                _logger.LogInformation("Starting CoinAPIPricingService...");
                _coinAPIPricingService.Start();

                _logger.LogInformation("Starting CoinAPIQuoteService...");
                _coinAPIQuoteService.Start();

                _logger.LogInformation("Starting NeovestSharesImbalanceService...");
                _neovestSharesImbalanceService.Start();

                _logger.LogInformation("Starting NeovestTradeVolumeService...");
                _neovestTradeVolumeService.Start();
            }
            else if (_configuration["ConnectionStrings:ENV"].Equals("DEV"))
            {
                _logger.LogInformation("Starting NeovestStatusService...");
                _neovestStatusService.Start();

                _logger.LogInformation("Starting NeovestOrderSummaryService [Simulated]...");
                _simNeovestOrderSummaryService.Start();

                //_logger.LogInformation("Starting NeovestSharesImbalanceService...");
                //_neovestSharesImbalanceService.Start();

                //_logger.LogInformation("Starting NeovestTradeVolumeService...");
                //_neovestTradeVolumeService.Start();
            }
            _logger.LogInformation("Starting Pricing Service(s)... - DONE");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="mainOrderSummaryDict"></param>
        /// <param name="orderSummaryDict"></param>
        /// <param name="orderExecutionDict"></param>
        private void PopulateLatestOrders(IDictionary<string, MainOrderSummary> mainOrderSummaryDict
            , IDictionary<string, OrderSummary> orderSummaryDict
            , IDictionary<string, OrderSummary> orderExecutionDict)
        {
            foreach (KeyValuePair<string, OrderSummary> kvp in orderSummaryDict)
            {
                OrderSummary orderSummary = kvp.Value;
                int orderRank = orderSummary.OrdRank;
                if (orderRank == 1)
                {
                    if (!mainOrderSummaryDict.TryGetValue(orderSummary.MainOrdId, out MainOrderSummary mainOrderSummary))
                    {
                        mainOrderSummary = new MainOrderSummary
                        {
                            MainOrderId = orderSummary.MainOrdId,
                            OrderId = orderSummary.OrdId,
                            OrderStatus = orderSummary.OrdSt,
                            ALMOrderStatus = orderSummary.ALMOrdSts,
                            MainOrderAvgTradedPrice = orderSummary.AvgTrdPr
                        };

                        if (!mainOrderSummary.OrderIdList.Contains(orderSummary.OrdId))
                            mainOrderSummary.OrderIdList.Add(orderSummary.OrdId);

                        mainOrderSummaryDict.Add(mainOrderSummary.MainOrderId, mainOrderSummary);
                    }
                }
                else
                {
                    if (mainOrderSummaryDict.TryGetValue(orderSummary.MainOrdId, out MainOrderSummary mainOrderSummary))
                    {
                        if (!mainOrderSummary.OrderIdList.Contains(orderSummary.OrdId))
                            mainOrderSummary.OrderIdList.Add(orderSummary.OrdId);
                    }
                }
            }

            foreach (KeyValuePair<string, OrderSummary> kvp in orderExecutionDict)
            {
                OrderSummary orderSummary = kvp.Value;
                if (mainOrderSummaryDict.TryGetValue(orderSummary.MainOrdId, out MainOrderSummary mainOrderSummary))
                {
                    if (!mainOrderSummary.ExecutionIdList.Contains(orderSummary.ExecId))
                        mainOrderSummary.ExecutionIdList.Add(orderSummary.ExecId);
                }
            }
        }

        /// <summary>
        /// Populate Order Details (PRODUCTION)
        /// </summary>
        private void PopulateOrderDetailsBySymbol()
        {
            IDictionary<string, IDictionary<string, OrderSummary>> orderSummaryBySymbolDict = new Dictionary<string, IDictionary<string, OrderSummary>>(StringComparer.CurrentCultureIgnoreCase);
            IDictionary<string, OrderSummary> orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_SUMMARY);
            foreach (KeyValuePair<string, OrderSummary> kvp in orderSummaryDict)
            {
                OrderSummary data = kvp.Value;

                string key;
                if (!string.IsNullOrEmpty(data.ParOrdId))
                    key = data.OrdId + "|" + "PAIR_TRADE";
                else
                    key = data.BBGSym + "|" + data.OrdSide;

                IDictionary<string, OrderSummary> dict;
                if (orderSummaryBySymbolDict.TryGetValue(key, out dict))
                {
                    if (!dict.ContainsKey(data.OrdId))
                        dict.Add(data.OrdId, data);
                }
                else
                {
                    dict = new Dictionary<string, OrderSummary>
                    {
                        { data.OrdId, data }
                    };
                    orderSummaryBySymbolDict.Add(key, dict);
                }
            }
            _cache.Add(CacheKeys.ORDER_SUMMARY_BY_SYMBOL, orderSummaryBySymbolDict, DateTimeOffset.MaxValue);
        }

        /// <summary>
        /// Populate Order Details (SIMULATION)
        /// </summary>
        private void PopulateSimulatedOrderDetailsBySymbol()
        {
            IDictionary<string, IDictionary<string, OrderSummary>> orderSummaryBySymbolDict = new Dictionary<string, IDictionary<string, OrderSummary>>(StringComparer.CurrentCultureIgnoreCase);
            IDictionary<string, OrderSummary> orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.SIM_ORDER_SUMMARY);
            foreach (KeyValuePair<string, OrderSummary> kvp in orderSummaryDict)
            {
                OrderSummary data = kvp.Value;

                string key;
                if (!string.IsNullOrEmpty(data.ParOrdId))
                    key = data.OrdId + "|" + "PAIR_TRADE";
                else
                    key = data.BBGSym + "|" + data.OrdSide;

                IDictionary<string, OrderSummary> dict;
                if (orderSummaryBySymbolDict.TryGetValue(key, out dict))
                {
                    if (!dict.ContainsKey(data.OrdId))
                        dict.Add(data.OrdId, data);
                }
                else
                {
                    dict = new Dictionary<string, OrderSummary>
                    {
                        { data.OrdId, data }
                    };
                    orderSummaryBySymbolDict.Add(key, dict);
                }
            }

            _cache.Remove(CacheKeys.SIM_ORDER_SUMMARY_BY_SYMBOL);
            _cache.Add(CacheKeys.SIM_ORDER_SUMMARY_BY_SYMBOL, orderSummaryBySymbolDict, DateTimeOffset.MaxValue);
        }

        /// <summary>
        /// 
        /// </summary>
        private void PopulateBatchOrders()
        {
            IList<NewOrder> batchOrders = _tradingDao.GetBatchOrders();
            _cache.Add(CacheKeys.BATCH_ORDERS, batchOrders, DateTimeOffset.MaxValue);

            IDictionary<string, NewOrder> batchOrdersByRefIdDict = new Dictionary<string, NewOrder>(StringComparer.CurrentCultureIgnoreCase);
            foreach (NewOrder nOrder in batchOrders)
            {
                if (!string.IsNullOrEmpty(nOrder.Id) &&
                    !batchOrdersByRefIdDict.ContainsKey(nOrder.Id))
                    batchOrdersByRefIdDict.Add(nOrder.Id, nOrder);
            }

            _cache.Remove(CacheKeys.BATCH_ORDERS_REFID_MAP);
            _cache.Add(CacheKeys.BATCH_ORDERS_REFID_MAP, batchOrdersByRefIdDict, DateTimeOffset.MaxValue);

            //Pair Trades
            IDictionary<string, NewPairOrder> pairTradeDict = new Dictionary<string, NewPairOrder>(StringComparer.CurrentCultureIgnoreCase);
            foreach (NewOrder nOrder in batchOrders)
            {
                if (!string.IsNullOrEmpty(nOrder.ParentId))
                {
                    if (pairTradeDict.TryGetValue(nOrder.ParentId, out NewPairOrder pairTrade))
                    {
                        if (nOrder.OrderSide.Equals("B", StringComparison.CurrentCultureIgnoreCase)
                            || nOrder.OrderSide.Equals("Buy", StringComparison.CurrentCultureIgnoreCase)
                            || nOrder.OrderSide.Equals("BC", StringComparison.CurrentCultureIgnoreCase)
                            || nOrder.OrderSide.Equals("Buy Cover", StringComparison.CurrentCultureIgnoreCase)
                            )
                            pairTrade.BuyOrder = nOrder;
                        else if (nOrder.OrderSide.Equals("S", StringComparison.CurrentCultureIgnoreCase)
                            || nOrder.OrderSide.Equals("Sell", StringComparison.CurrentCultureIgnoreCase)
                            || nOrder.OrderSide.Equals("SS", StringComparison.CurrentCultureIgnoreCase)
                            || nOrder.OrderSide.Equals("Sell Short", StringComparison.CurrentCultureIgnoreCase)
                            || nOrder.OrderSide.Equals("SSx", StringComparison.CurrentCultureIgnoreCase)
                            )
                            pairTrade.SellOrder = nOrder;
                    }
                }
            }

            _cache.Remove(CacheKeys.PAIR_TRADE_MAP);
            _cache.Add(CacheKeys.PAIR_TRADE_MAP, pairTradeDict, DateTimeOffset.MaxValue);

            IDictionary<string, string> refIdOrderDict = new Dictionary<string, string>();
            _cache.Remove(CacheKeys.REFID_ORDERS_MAP);
            _cache.Add(CacheKeys.REFID_ORDERS_MAP, refIdOrderDict, DateTimeOffset.MaxValue);
        }

        /// <summary>
        /// 
        /// </summary>
        private void PopulateSimBatchOrders()
        {
            IList<NewOrder> batchOrders = _tradingDao.GetSimBatchOrders();
            _cache.Add(CacheKeys.SIM_BATCH_ORDERS, batchOrders, DateTimeOffset.MaxValue);

            IDictionary<string, NewOrder> batchOrdersByRefIdDict = _cache.Get<IDictionary<string, NewOrder>>(CacheKeys.BATCH_ORDERS_REFID_MAP);
            foreach (NewOrder nOrder in batchOrders)
            {
                if (!string.IsNullOrEmpty(nOrder.Id) &&
                    !batchOrdersByRefIdDict.ContainsKey(nOrder.Id))
                    batchOrdersByRefIdDict.Add(nOrder.Id, nOrder);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void PopulatePairOrders()
        {
            IDictionary<string, PairOrder> dict = _pairTradingDao.GetPairOrders();
            _cache.Add(CacheKeys.PAIR_TRADE_BATCH_ORDERS, dict, DateTimeOffset.MaxValue);

            IDictionary<string, NewPairOrder> newPairOrderDict = _pairTradingNVDao.GetPairOrders();
            _cache.Add(CacheKeys.NV_PAIR_TRADE_BATCH_ORDERS, newPairOrderDict, DateTimeOffset.MaxValue);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="orderSummaryDict"></param>
        private void PopulateRefIndex(IDictionary<string, OrderSummary> orderSummaryDict)
        {
            string almSymbol = string.Empty;
            try
            {
                IDictionary<string, TradingTarget> tradingTargetDict = _cache.Get<IDictionary<string, TradingTarget>>(CacheKeys.TRADING_TARGETS);
                foreach (KeyValuePair<string, OrderSummary> kvp in orderSummaryDict)
                {
                    OrderSummary orderSummary = kvp.Value;
                    almSymbol = orderSummary.ALMSym;
                    if (!string.IsNullOrEmpty(almSymbol) && tradingTargetDict.TryGetValue(almSymbol, out TradingTarget tradingTarget))
                    {
                        string refIndex = tradingTarget.RefIndex;
                        IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);
                        IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);

                        SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(refIndex, priceTickerMap, securityPriceDict);
                        if (securityPrice != null)
                        {
                            orderSummary.RI = refIndex;
                            orderSummary.RILastPr = securityPrice.ClsPrc;
                            orderSummary.RIBeta = tradingTarget.PriceBeta;
                            orderSummary.RIPrCap = tradingTarget.PriceCap;
                            orderSummary.RIPrBetaShiftInd = tradingTarget.PriceBetaShiftInd;
                            orderSummary.RIPrCapShiftInd = tradingTarget.PriceCapShiftInd;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error populating Ref Index for Symbol: " + almSymbol);
            }
        }
    }
}