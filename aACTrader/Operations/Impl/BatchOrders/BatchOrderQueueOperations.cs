using aACTrader.Services.Neovest;
using aCommons;
using aCommons.Cef;
using aCommons.Trading;
using LazyCache;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Operations.Impl.BatchOrders
{
    public class BatchOrderQueueOperations
    {
        private readonly ILogger<BatchOrderQueueOperations> _logger;
        private readonly CachingService _cache;
        private readonly SimNeovestOrderPublisher _simNeovestOrderPublisher;
        private readonly NeovestOrderPublisher _neovestOrderPublisher;
        private readonly IConfiguration _configuration;
        private readonly DiscountTargetOrder _discountTargetOrder;
        private readonly RefIndexOrder _refIndexOrder;
        private readonly OrderValidator _orderValidator;

        protected static readonly int ORDER_UPDATE_COUNT = 10;
        protected static readonly double DEFAULT_PRICE_CAP = 0.03;
        protected static readonly double DEFAULT_MIN_ORDER_UPDATE_THRESHOLD = 5; //bps
        protected static readonly double DEFAULT_MAX_ORDER_UPDATE_THRESHOLD = 50; //bps
        protected static readonly double DEFAULT_MARKET_PRICE_THRESHOLD = 0.005;
        protected static readonly double DISCOUNT_DEFAULT_MARKET_PRICE_THRESHOLD = 0.010;
        private readonly Random RandomNumberGenerator = new Random();
        private static readonly DateTime TODAYS_DATE = DateTime.Today;

        public BatchOrderQueueOperations(ILogger<BatchOrderQueueOperations> logger
            , CachingService cache
            , SimNeovestOrderPublisher simNeovestOrderPublisher
            , NeovestOrderPublisher neovestOrderPublisher
            , IConfiguration configuration
            , DiscountTargetOrder discountTargetOrder
            , RefIndexOrder refIndexOrder
            , OrderValidator orderValidator)
        {
            this._logger = logger;
            this._cache = cache;
            this._simNeovestOrderPublisher = simNeovestOrderPublisher;
            this._neovestOrderPublisher = neovestOrderPublisher;
            this._configuration = configuration;
            this._discountTargetOrder = discountTargetOrder;
            this._refIndexOrder = refIndexOrder;
            this._orderValidator = orderValidator;
            _logger.LogInformation("Initializing BatchOrderQueueOperations...");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="orderList"></param>
        public void ProcessNewOrders(IList<NewOrder> orderList)
        {
            try
            {
                if (orderList != null && orderList.Count > 0)
                {
                    IList<NewOrder> batchOrders = _cache.Get<IList<NewOrder>>(CacheKeys.BATCH_ORDERS);
                    IDictionary<string, NewOrder> batchOrdersByRefIdDict = _cache.Get<IDictionary<string, NewOrder>>(CacheKeys.BATCH_ORDERS_REFID_MAP);
                    foreach (NewOrder order in orderList)
                    {
                        //Add to Batch Orders List
                        batchOrders.Add(order);

                        //Add to Batch Orders RefId Map
                        if (!string.IsNullOrEmpty(order.Id) &&
                            !batchOrdersByRefIdDict.ContainsKey(order.Id))
                            batchOrdersByRefIdDict.Add(order.Id, order);

                        //Process New Order
                        if ("New".Equals(order.ActionType, StringComparison.CurrentCultureIgnoreCase))
                        {
                            _logger.LogInformation("Processing NEW Order: " + order.Symbol + "/" + order.Id);
                            ProcessNewOrder(order);
                            PopulateNewOrder(order);

                            //Populate Trader Name
                            if (string.IsNullOrEmpty(order.UserName))
                                order.UserName = "almitas-api-prod";
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing NEW orders");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="orderList"></param>
        public void ProcessNewOrdersSim(IList<NewOrder> orderList)
        {
            try
            {
                if (orderList.Count > 0)
                {
                    IList<NewOrder> batchOrders = _cache.Get<IList<NewOrder>>(CacheKeys.SIM_BATCH_ORDERS);
                    IDictionary<string, NewOrder> batchOrdersByRefIdDict = _cache.Get<IDictionary<string, NewOrder>>(CacheKeys.BATCH_ORDERS_REFID_MAP);
                    foreach (NewOrder order in orderList)
                    {
                        //Add to Batch Orders List
                        batchOrders.Add(order);

                        //Add to Batch Orders RefId Map
                        if (!string.IsNullOrEmpty(order.Id) &&
                            !batchOrdersByRefIdDict.ContainsKey(order.Id))
                            batchOrdersByRefIdDict.Add(order.Id, order);

                        //Process New Order
                        if ("New".Equals(order.ActionType, StringComparison.CurrentCultureIgnoreCase))
                        {
                            _logger.LogInformation("[Simulation] Processing NEW Order: " + order.Symbol + "/" + order.Id);
                            ProcessNewOrder(order);
                            PopulateNewOrder(order);

                            //Populate Trader Name
                            if (string.IsNullOrEmpty(order.UserName))
                                order.UserName = "almitas-api-test";
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing NEW orders[Simulated]");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="order"></param>
        private void PopulateNewOrder(NewOrder order)
        {
            IDictionary<string, OrderSummary> orderSummaryDict;
            if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_QUEUE_SUMMARY);
            else
                orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.SIM_ORDER_QUEUE_SUMMARY);

            OrderSummary orderSummary = new OrderSummary()
            {
                Action = order.ActionType,
                RefId = order.Id,
                Sym = order.Symbol,
                ALMSym = order.Symbol,
                BBGSym = order.Symbol,
                OrdSide = order.OrderSide,
                OrdTyp = order.OrderType,
                OrdSt = order.OrderStatus,
                AccName = order.AccountName,
                OrdDest = order.Destination,
                OrdBkrStrat = order.BrokerStrategy,
                OrdOrigPr = order.OrigOrderPrice,
                OrdPr = order.OrderPrice,
                OrdStopPr = order.OrderStopPrice,
                OrdQty = (int?)order.OrderQty,
                Trader = order.UserName,
                AlgoParameters = order.AlgoParameters,
                Locate = order.Locate,
                NewOrdPr = order.NewOrderPrice,

                RI = order.RefIndex,
                RIPrTyp = order.RefIndexPriceType,
                RIBeta = order.RefIndexPriceBeta,
                RIBetaAdjTyp = order.RefIndexBetaAdjType,
                RIPrCap = order.RefIndexPriceCap,
                RIPrBetaShiftInd = order.RefIndexPriceBetaShiftInd,
                RIPrCapShiftInd = order.RefIndexPriceCapShiftInd,
                RIMaxPr = order.RefIndexMaxPrice,

                DscntTgt = order.DiscountTarget,
                DscntTgtAdj = order.DiscountTargetAdj,
                EstNavTyp = order.EstNavType,

                AutoUpdate = order.AutoUpdate,
                AutoUpdateThld = order.AutoUpdateThreshold,
                MktPrThld = order.MarketPriceThreshold,
                MktPrFld = order.MarketPriceField,
                UpdateAlgoParams = order.UpdateAlgoParams,

                IsQueueOrd = 1,
            };

            orderSummary.OrdDt = TODAYS_DATE;
            orderSummary.Action = "Pending";
            orderSummary.OrdSt = "Pending";
            orderSummary.OrdTm = TODAYS_DATE.ToString("HH:mm:ss tt");

            if (!orderSummaryDict.ContainsKey(orderSummary.RefId))
                orderSummaryDict.Add(orderSummary.RefId, orderSummary);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="order"></param>
        private bool ProcessNewOrder(NewOrder order)
        {
            bool validOrder = true;

            try
            {
                //Populate Reference Id Beta Shift Indicator
                if (!string.IsNullOrEmpty(order.RefIndexPriceBetaShiftIndAsString))
                {
                    if (order.RefIndexPriceBetaShiftIndAsString.Equals("Both", StringComparison.CurrentCultureIgnoreCase))
                        order.RefIndexPriceBetaShiftInd = 0;
                    else if (order.RefIndexPriceBetaShiftIndAsString.Equals("Up", StringComparison.CurrentCultureIgnoreCase))
                        order.RefIndexPriceBetaShiftInd = 1;
                    else if (order.RefIndexPriceBetaShiftIndAsString.Equals("Down", StringComparison.CurrentCultureIgnoreCase))
                        order.RefIndexPriceBetaShiftInd = -1;
                }

                //Populate Reference Id/Discount Target Price Cap Shift Indicator
                if (!string.IsNullOrEmpty(order.RefIndexPriceCapShiftIndAsString))
                {
                    if (order.RefIndexPriceCapShiftIndAsString.Equals("Both", StringComparison.CurrentCultureIgnoreCase))
                        order.RefIndexPriceCapShiftInd = 0;
                    else if (order.RefIndexPriceCapShiftIndAsString.Equals("Up", StringComparison.CurrentCultureIgnoreCase))
                        order.RefIndexPriceCapShiftInd = 1;
                    else if (order.RefIndexPriceCapShiftIndAsString.Equals("Down", StringComparison.CurrentCultureIgnoreCase))
                        order.RefIndexPriceCapShiftInd = -1;
                }

                //Populate Order Side Indicator
                if (!string.IsNullOrEmpty(order.OrderSide))
                {
                    if (order.OrderSide.Equals("B", StringComparison.CurrentCultureIgnoreCase) || order.OrderSide.Equals("BC", StringComparison.CurrentCultureIgnoreCase))
                        order.OrderSideInd = 1;
                    else if (order.OrderSide.Equals("S", StringComparison.CurrentCultureIgnoreCase) || order.OrderSide.Equals("SS", StringComparison.CurrentCultureIgnoreCase))
                        order.OrderSideInd = 2;
                }

                //Calculate Order Price if Ref Index is provided/Discount Target is provided
                //This is to capture any price movement by the time Order is sent to Neovest
                if (order.RefIndexPrice.HasValue && order.RefIndexPrice.GetValueOrDefault() > 0)
                    CalculateRefIndexOrderPrice(order);
                else if (order.DiscountTarget.HasValue)
                    CalculateDiscountOrderPrice(order);

                //Round Order Price (>= 0.4)
                if (order.OrderPrice.GetValueOrDefault() >= 0.4)
                {
                    double givenOrderPrice = order.OrderPrice.GetValueOrDefault();
                    //BUY
                    if (order.OrderSideInd.GetValueOrDefault() == 1)
                    {
                        order.OrigOrderPrice = order.OrderPrice;
                        order.OrderPrice = Math.Floor(givenOrderPrice * 100.0) / 100.0;
                        order.OrderLimitPrice = order.OrderPrice;
                        order.OrderStopPrice = order.OrderPrice;
                    }
                    //SELL
                    else if (order.OrderSideInd.GetValueOrDefault() == 2)
                    {
                        order.OrigOrderPrice = order.OrderPrice;
                        order.OrderPrice = Math.Ceiling(givenOrderPrice * 100.0) / 100.0;
                        order.OrderLimitPrice = order.OrderPrice;
                        order.OrderStopPrice = order.OrderPrice;
                    }

                    _logger.LogInformation("Rounding Order Price for Id: "
                        + order.Id + "/" + order.OrderSide + "/" + givenOrderPrice + "/" + order.OrderPrice);
                }

                //Derive Neovest Symbol using Bloomberg Symbol
                if (!string.IsNullOrEmpty(order.IsOption) && "Y".Equals(order.IsOption))
                {
                    string neovestSymbol = NeovestOperations.GetNeovestSymbol(order.Symbol);
                    if (!string.IsNullOrEmpty(neovestSymbol))
                    {
                        order.NeovestSymbol = neovestSymbol;
                        _logger.LogInformation("Neovest Symbol for Input Symbol: " + neovestSymbol + "/" + order.Symbol);
                    }
                    else
                    {
                        _logger.LogError("Error generating Neovest Symbol: " + order.Id + "/" + order.Symbol);
                    }
                }

                //Default Market Price Threshold (if missing)
                if (!order.MarketPriceThreshold.HasValue)
                {
                    if (order.DiscountTarget.HasValue)
                        order.MarketPriceThreshold = DISCOUNT_DEFAULT_MARKET_PRICE_THRESHOLD;
                    else
                        order.MarketPriceThreshold = DEFAULT_MARKET_PRICE_THRESHOLD;
                }

                //Populate Market Price Field
                if (string.IsNullOrEmpty(order.MarketPriceField))
                {
                    order.MarketPriceField = "Bid"; //default
                    if (order.OrderSideInd.GetValueOrDefault() == 1)
                        order.MarketPriceField = "Bid";
                    else if (order.OrderSideInd.GetValueOrDefault() == 2)
                        order.MarketPriceField = "Ask";
                }
            }
            catch (Exception ex)
            {
                validOrder = false;
                _logger.LogError("Error processing the Order: " + order.Symbol + "/" + order.Id, ex);
            }

            return validOrder;
        }

        /// <summary>
        /// 
        /// </summary>
        public void ProcessRefIndexOrders()
        {
            //_logger.LogInformation("Processing Ref Index Orders [Order Queue] - STARTED");
            string environment = "PRODUCTION";
            int orderCount = 0;

            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);
            IDictionary<string, OrderSummary> orderSummaryDict;

            if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
            {
                orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_QUEUE_SUMMARY);
            }
            else
            {
                orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.SIM_ORDER_QUEUE_SUMMARY);
                environment = "SIMULATION";
            }

            //list of orders to submit to Neovest
            IList<NewOrder> updateOrders = new List<NewOrder>();

            foreach (KeyValuePair<string, OrderSummary> kvp in orderSummaryDict)
            {
                string referenceId = kvp.Key;
                OrderSummary orderSummary = kvp.Value;

                try
                {
                    //check if the order is reference index order
                    //check if the order is pending
                    if (orderSummary.Action.Equals("Pending") && !string.IsNullOrEmpty(orderSummary.RI))
                    {
                        orderCount++;

                        //get reference index price
                        SecurityPrice refIndexPrice = SecurityPriceLookupOperations.GetSecurityPrice(orderSummary.RI, priceTickerMap, securityPriceDict);
                        if (refIndexPrice != null)
                        {
                            //get live price (last/mid/bid/ask)
                            double? livePrice = null;
                            switch (orderSummary.RIPrTyp)
                            {
                                case "LAST":
                                    livePrice = refIndexPrice.LastPrc;
                                    break;
                                case "MID":
                                    livePrice = refIndexPrice.MidPrc;
                                    break;
                                case "BID":
                                    livePrice = refIndexPrice.BidPrc;
                                    break;
                                case "ASK":
                                    livePrice = refIndexPrice.AskPrc;
                                    break;
                            }

                            if (refIndexPrice.BidPrc <= refIndexPrice.AskPrc)
                            {
                                //gets previous target price if the order is replaced to calculate new order price
                                //this is done to avoid applying the price change on rounded order price
                                //as this could make order drift away from market price
                                double? previousTargetPrice = orderSummary.OrdPr;
                                if (!string.IsNullOrEmpty(orderSummary.CancOrdId))
                                {
                                    if (orderSummaryDict.TryGetValue(orderSummary.CancOrdId, out OrderSummary prevOrderSummary))
                                    {
                                        if (prevOrderSummary.RITgtPr.HasValue
                                            && prevOrderSummary.RITgtPr.GetValueOrDefault() > 0)
                                            previousTargetPrice = prevOrderSummary.RITgtPr;
                                        else
                                            _logger.LogInformation("Invalid Order Target Price for OrderId: " + orderSummary.CancOrdId);
                                    }
                                    else
                                    {
                                        _logger.LogInformation("Could not find Order Target Price for OrderId: " + orderSummary.CancOrdId);
                                    }
                                }

                                if (livePrice.HasValue
                                    && livePrice.GetValueOrDefault() > 0
                                    && orderSummary.RILastPr.HasValue
                                    && orderSummary.RILastPr.GetValueOrDefault() > 0)
                                {
                                    //get live price of security
                                    //this is added to make sure the bid price is less than offer price for BUY orders
                                    //and offer price is more than bid price for SELL orders
                                    string symbol = !string.IsNullOrEmpty(orderSummary.ALMSym) ? orderSummary.ALMSym : orderSummary.BBGSym;
                                    SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(symbol, priceTickerMap, securityPriceDict);

                                    //applies beta on price change
                                    if ("Pct".Equals(orderSummary.RIBetaAdjTyp, StringComparison.CurrentCultureIgnoreCase))
                                        _refIndexOrder.ProcessRefIndexBetaOrder(orderSummary, livePrice, previousTargetPrice, securityPrice);
                                    //applies delta on underlying price
                                    else if ("Delta".Equals(orderSummary.RIBetaAdjTyp, StringComparison.CurrentCultureIgnoreCase))
                                        _refIndexOrder.ProcessRefIndexDeltaOrder(orderSummary, livePrice, previousTargetPrice, securityPrice);
                                    //applies abs diff on underlying price
                                    else if ("Abs".Equals(orderSummary.RIBetaAdjTyp, StringComparison.CurrentCultureIgnoreCase))
                                        _refIndexOrder.ProcessRefIndexAbsOrder(orderSummary, livePrice, previousTargetPrice, securityPrice);

                                    _logger.LogInformation("Processing Order: "
                                        + orderSummary.Action + "/"
                                        + orderSummary.RefId + "/"
                                        + orderSummary.Sym + "/"
                                        + orderSummary.OrdPr + "/"
                                        + orderSummary.RITgtPr);

                                    //check if order is to be submitted
                                    if (CheckForMarketPriceThreshold(orderSummary))
                                        CreateUpdateOrder(orderSummary, environment, updateOrders);
                                }
                            }
                            else
                            {
                                _logger.LogInformation("Ref Index Bid Price > Ask Price: "
                                    + orderSummary.Sym + "/"
                                    + orderSummary.RefId + "/"
                                    + orderSummary.RI + "/"
                                    + refIndexPrice.LastPrc + "/"
                                    + refIndexPrice.BidPrc + "/"
                                    + refIndexPrice.AskPrc + "/"
                                    + orderSummary.BidAskPrFlag);

                                orderSummary.BidAskPrFlag++;
                                if (orderSummary.BidAskPrFlag > 5)
                                {
                                    _logger.LogInformation("Canceling Order (Bid > Ask): "
                                        + orderSummary.Sym + "/"
                                        + orderSummary.RefId + "/"
                                        + orderSummary.RI + "/"
                                        + refIndexPrice.LastPrc + "/"
                                        + refIndexPrice.BidPrc + "/"
                                        + refIndexPrice.AskPrc + "/"
                                        + orderSummary.BidAskPrFlag);
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing Ref Index order: " + referenceId);
                }
            }

            ////submit updated orders
            //if (updateOrders.Count > 0)
            //{
            //    if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
            //        ProcessNewOrders(updateOrders);
            //    else
            //        ProcessNewOrdersSim(updateOrders);
            //}

            _logger.LogInformation("Processed Ref Index Orders: " + orderCount);
        }

        /// <summary>
        /// 
        /// </summary>
        public void ProcessDiscountOrders()
        {
            //_logger.LogInformation("Process Discount Orders [Orders Queue] - STARTED");
            string environment = "PRODUCTION";
            int orderCount = 0;

            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
            IDictionary<string, OrderSummary> orderSummaryDict;

            if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
            {
                orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_QUEUE_SUMMARY);
            }
            else
            {
                orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.SIM_ORDER_QUEUE_SUMMARY);
                environment = "SIMULATION";
            }

            //list of orders to auto-update
            IList<NewOrder> updateOrders = new List<NewOrder>();

            foreach (KeyValuePair<string, OrderSummary> kvp in orderSummaryDict)
            {
                string referenceId = kvp.Key;
                OrderSummary orderSummary = kvp.Value;

                try
                {

                    //check if the order is discount order
                    //check if the order is pending
                    if (orderSummary.Action.Equals("Pending") && orderSummary.DscntTgt.HasValue)
                    {
                        orderCount++;

                        //gets previous target price if the order is replaced to calculate new order price
                        //this is done to avoid applying the price change on rounded order price
                        //as this could make order drift away from market price
                        double? previousTargetPrice = orderSummary.OrdPr;
                        if (!string.IsNullOrEmpty(orderSummary.CancOrdId))
                        {
                            if (orderSummaryDict.TryGetValue(orderSummary.CancOrdId, out OrderSummary prevOrderSummary))
                            {
                                if (prevOrderSummary.RITgtPr.HasValue
                                    && prevOrderSummary.RITgtPr.GetValueOrDefault() > 0)
                                    previousTargetPrice = prevOrderSummary.RITgtPr;
                                else
                                    _logger.LogInformation("Invalid Order Target Price for OrderId: " + orderSummary.CancOrdId);
                            }
                            else
                            {
                                _logger.LogInformation("Could not find Order Target Price for OrderId: " + orderSummary.CancOrdId);
                            }
                        }

                        //get live price of security
                        string symbol = !string.IsNullOrEmpty(orderSummary.ALMSym) ? orderSummary.ALMSym : orderSummary.BBGSym;
                        SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(symbol, priceTickerMap, securityPriceDict);

                        //process discount order
                        _discountTargetOrder.ProcessDiscountOrder(orderSummary, fundForecastDict, previousTargetPrice, securityPrice);

                        //check if order is to be updated
                        if (CheckForMarketPriceThreshold(orderSummary))
                            CreateUpdateOrder(orderSummary, environment, updateOrders);

                        orderSummary.OrdStatusUpdTm = DateTime.Now.ToString("HH:mm:ss tt");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing Order for Ref Index/Discount Target change: " + referenceId);
                }
            }

            ////submit updated orders
            //if (updateOrders.Count > 0)
            //{
            //    if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
            //        ProcessNewOrders(updateOrders);
            //    else
            //        ProcessNewOrdersSim(updateOrders);
            //}

            _logger.LogInformation("Processed Discount Orders: " + orderCount);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="order"></param>
        /// <returns></returns>
        private bool CheckForMarketPriceThreshold(OrderSummary order)
        {
            bool marketPriceThresholdFlag = false;

            try
            {
                order.TrdFlag = null;
                if (order.MktPrThld.HasValue)
                {
                    if (Math.Abs(order.MktPrSprd.GetValueOrDefault()) > order.MktPrThld.GetValueOrDefault())
                    {
                        marketPriceThresholdFlag = true;
                        _logger.LogInformation("[Outside Price Threshold]: "
                            + order.Sym + "/"
                            + order.MainOrdId + "/"
                            + order.OrdId + "/"
                            + order.OrdPr + "/"
                            + order.NewOrdPr + "/"
                            + order.BidPr + "/"
                            + order.AskPr + "/"
                            + order.MktPrSprd + "/"
                            + order.MktPrThld);
                    }
                    else
                    {
                        order.TrdFlag = "YES";
                    }
                }

                if (!marketPriceThresholdFlag)
                {
                    _logger.LogInformation("[Price Threshold Check]: "
                                + order.Sym + "/"
                                + order.MainOrdId + "/"
                                + order.OrdId + "/"
                                + order.OrdPr + "/"
                                + order.NewOrdPr + "/"
                                + order.BidPr + "/"
                                + order.AskPr + "/"
                                + order.MktPrSprd + "/"
                                + order.MktPrThld);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating Market Price Spread for Order: " + order.RefId + "/" + order.RITgtPr);
            }

            return marketPriceThresholdFlag;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="order"></param>
        private void CalculateRefIndexOrderPrice(NewOrder order)
        {
            try
            {
                IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
                IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);

                SecurityPrice refIndexPrice = SecurityPriceLookupOperations.GetSecurityPrice(order.RefIndex, priceTickerMap, securityPriceDict);
                if (refIndexPrice != null)
                {
                    //get live price (last/mid/bid/ask)
                    double? livePrice = null;
                    switch (order.RefIndexPriceType)
                    {
                        case "LAST":
                            livePrice = refIndexPrice.LastPrc;
                            break;
                        case "MID":
                            livePrice = refIndexPrice.MidPrc;
                            break;
                        case "BID":
                            livePrice = refIndexPrice.BidPrc;
                            break;
                        case "ASK":
                            livePrice = refIndexPrice.AskPrc;
                            break;
                    }

                    if (livePrice.HasValue && livePrice.GetValueOrDefault() > 0)
                    {
                        double priceChange = (livePrice.GetValueOrDefault() / order.RefIndexPrice.GetValueOrDefault()) - 1.0;
                        if (order.OrderSideInd.GetValueOrDefault() == 1 && priceChange < 0)
                        {
                            order.OrigOrderPrice = order.OrderPrice;
                            order.OrderPrice = order.OrderPrice.GetValueOrDefault() * (1.0 + priceChange);
                        }
                        else if (order.OrderSideInd.GetValueOrDefault() == 2 && priceChange > 0)
                        {
                            order.OrigOrderPrice = order.OrderPrice;
                            order.OrderPrice = order.OrderPrice.GetValueOrDefault() * (1.0 + priceChange);
                        }

                        _logger.LogInformation("Updating Order Price for Id: " + order.Id + "/" + order.OrigOrderPrice + "/" + order.OrderPrice);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating Order Price using Ref Index Price");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="order"></param>
        private void CalculateDiscountOrderPrice(NewOrder order)
        {
            try
            {
                IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);

                string symbol = order.Symbol;
                double newPrice = order.OrderPrice.GetValueOrDefault();
                FundForecast fundForecast = FundForecastOperations.GetFundForecast(symbol, fundForecastDict);
                if (fundForecast != null)
                {
                    //double? nav = fundForecast.EstNav;
                    double? nav = _discountTargetOrder.GetEstimatedNav(order.EstNavType, fundForecast);
                    if (nav.HasValue)
                    {
                        //New Price (based on Estimated Nav and Discount Target)
                        double discountTarget = order.DiscountTarget.GetValueOrDefault() + order.DiscountTargetAdj.GetValueOrDefault();
                        newPrice = (1.0 + discountTarget) * nav.GetValueOrDefault();

                        if (newPrice > 0)
                        {
                            double priceChange = (newPrice / order.OrderPrice.GetValueOrDefault()) - 1.0;
                            if (order.OrderSideInd.GetValueOrDefault() == 1 && priceChange < 0)
                            {
                                order.OrigOrderPrice = order.OrderPrice;
                                order.OrderPrice = order.OrderPrice.GetValueOrDefault() * (1.0 + priceChange);
                            }
                            else if (order.OrderSideInd.GetValueOrDefault() == 2 && priceChange > 0)
                            {
                                order.OrigOrderPrice = order.OrderPrice;
                                order.OrderPrice = order.OrderPrice.GetValueOrDefault() * (1.0 + priceChange);
                            }

                            _logger.LogInformation("Updating Order Price for Id: " + order.Id + "/" + order.OrigOrderPrice + "/" + order.OrderPrice);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating Order Price using Ref Index Price");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="order"></param>
        private void SubmitOrderSim(NewOrder order)
        {
            //try
            //{
            //    _simNeovestOrderPublisher.PublishMessage(order);
            //}
            //catch (Exception ex)
            //{
            //    _logger.LogError(ex, "Error submitting New/Update orders [Simulation Environment]");
            //}
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="order"></param>
        private void SubmitOrder(NewOrder order)
        {
            //try
            //{
            //    _neovestOrderPublisher.PublishMessage(order);
            //}
            //catch (Exception ex)
            //{
            //    _logger.LogError(ex, "Error submitting New/Update orders [Production Environment]");
            //}
        }

        /// <summary>
        /// Check if the Order is ready to be replaced (Price/Qty change)
        /// Order can be replaced for max of 10 times (for now)
        /// </summary>
        /// <param name="orderSummary"></param>
        /// <returns></returns>
        protected bool CheckForAutoUpdate(OrderSummary orderSummary, MainOrderSummary mainOrderSummary)
        {
            bool result = false;

            try
            {
                if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                {
                    if ((orderSummary.OrdSt.Equals("Pending")
                        || orderSummary.OrdSt.Equals("Replaced")
                        || orderSummary.OrdSt.Equals("Replace Rejected")
                        || orderSummary.OrdSt.Equals("Partial"))
                    && string.IsNullOrEmpty(orderSummary.ALMOrdSts))
                    {
                        result = true;
                    }
                    else
                    {
                        _logger.LogInformation("CheckForAutoUpdate - "
                            + orderSummary.Sym + "/"
                            + orderSummary.MainOrdId + "/"
                            + orderSummary.OrdId + "/"
                            + orderSummary.OrdSt + "/"
                            + mainOrderSummary.OrderStatus);

                        //if (mainOrderSummary.OrderStatus.Equals("Pending")
                        //    || mainOrderSummary.OrderStatus.Equals("Replaced")
                        //    || mainOrderSummary.OrderStatus.Equals("Replace Rejected")
                        //    || mainOrderSummary.OrderStatus.Equals("Partial"))
                        //    return true;
                    }
                }
                else if (_configuration["ConnectionStrings:ENV"].Equals("DEV"))
                {
                    if ((orderSummary.OrdSt.Equals("Pending")
                        || orderSummary.OrdSt.Equals("Replaced")
                        || orderSummary.OrdSt.Equals("Replace Rejected")
                        || orderSummary.OrdSt.Equals("Partial"))
                    && string.IsNullOrEmpty(orderSummary.ALMOrdSts)
                    && mainOrderSummary.OrderIdList.Count < ORDER_UPDATE_COUNT)
                    {
                        result = true;
                    }
                    else
                    {
                        _logger.LogInformation("CheckForAutoUpdate - "
                            + orderSummary.Sym + "/"
                            + orderSummary.MainOrdId + "/"
                            + orderSummary.OrdId + "/"
                            + orderSummary.OrdSt + "/"
                            + mainOrderSummary.OrderStatus);

                        //if ((mainOrderSummary.OrderStatus.Equals("Pending")
                        //    || mainOrderSummary.OrderStatus.Equals("Replaced")
                        //    || mainOrderSummary.OrderStatus.Equals("Replace Rejected")
                        //    || mainOrderSummary.OrderStatus.Equals("Partial"))
                        //    && mainOrderSummary.UpdateOrderCount < ORDER_UPDATE_COUNT)
                        //    return true;
                    }
                }

                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error checking Order status");
                return result;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="orderSummary"></param>
        /// <param name="environment"></param>
        /// <param name="updateOrders"></param>
        protected void CreateUpdateOrder(OrderSummary orderSummary
            , string environment, IList<NewOrder> updateOrders)
        {
            try
            {
                double currentOrderPrice = orderSummary.OrdPr.GetValueOrDefault();
                double newOrderPrice = orderSummary.NewOrdPr.GetValueOrDefault();

                ///////////////////////TODO: Added for TESTING only
                //if (_configuration["ConnectionStrings:ENV"].Equals("DEV"))
                //{
                //    orderSummary.RefIndexTargetPrice *= (1.0 + RandomNumberGenerator.NextDouble() / 10.0);
                //    newOrderPrice = Math.Round(orderSummary.RefIndexTargetPrice.GetValueOrDefault(), 2);
                //}

                if (newOrderPrice > 0.0 && (currentOrderPrice != newOrderPrice))
                {
                    NewOrder updateOrder = new NewOrder();

                    updateOrder.ActionType = "Update";
                    updateOrder.Id = orderSummary.RefId;
                    updateOrder.MainOrderId = orderSummary.MainOrdId;
                    updateOrder.OrderId = orderSummary.OrdId;
                    updateOrder.Symbol = orderSummary.BBGSym;
                    updateOrder.NeovestSymbol = orderSummary.Sym;
                    updateOrder.OrderSide = orderSummary.OrdSide;
                    updateOrder.OrderType = orderSummary.OrdTyp;
                    updateOrder.OrderPrice = orderSummary.OrdPr;
                    updateOrder.OrderQty = orderSummary.OrdQty;
                    updateOrder.OrderStatus = orderSummary.OrdSt;
                    updateOrder.AccountName = orderSummary.AccName;
                    updateOrder.AccountId = orderSummary.AccNum;
                    updateOrder.OrderExchangeId = orderSummary.OrdExchId;
                    updateOrder.NewOrderPrice = newOrderPrice; // NEW Order Price
                    updateOrder.NewOrderQty = orderSummary.OrdQty; // NEW Order Qty
                    updateOrder.UserName = orderSummary.Trader;
                    updateOrder.Environment = environment;

                    //Ref Index and Discount Target Fields
                    updateOrder.RefIndex = orderSummary.RI;
                    updateOrder.RefIndexPriceType = orderSummary.RIPrTyp;
                    updateOrder.RefIndexPriceBeta = orderSummary.RIBeta;
                    updateOrder.RefIndexBetaAdjType = orderSummary.RIBetaAdjTyp;
                    updateOrder.RefIndexPriceBetaShiftInd = orderSummary.RIPrBetaShiftInd;
                    updateOrder.RefIndexPriceCap = orderSummary.RIPrCap;
                    updateOrder.RefIndexPriceCapShiftInd = orderSummary.RIPrCapShiftInd;
                    updateOrder.RefIndexMaxPrice = orderSummary.RIMaxPr;
                    updateOrder.DiscountTarget = orderSummary.DscntTgt;
                    updateOrder.AutoUpdate = orderSummary.AutoUpdate;
                    updateOrder.AutoUpdateThreshold = orderSummary.AutoUpdateThld;
                    updateOrder.MarketPriceThreshold = orderSummary.MktPrThld;
                    updateOrder.MarketPriceField = orderSummary.MktPrFld;
                    updateOrder.UpdateAlgoParams = orderSummary.UpdateAlgoParams;

                    //Update ALM Order Status
                    orderSummary.ALMOrdSts = "Replace Sent";

                    _logger.LogInformation("Updating Order: " +
                        string.Join("/"
                        , orderSummary.Sym
                        , orderSummary.MainOrdId
                        , orderSummary.OrdId
                        , orderSummary.CancOrdId
                        , orderSummary.OrdSt
                        , orderSummary.OrdPr
                        , orderSummary.NewOrdPr));

                    updateOrders.Add(updateOrder);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error creating Update Order");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="orderList"></param>
        public void UpdateOrders(IList<NewOrder> orderList)
        {
            try
            {
                if (orderList != null && orderList.Count > 0)
                {
                    IDictionary<string, NewOrder> batchOrdersByRefIdDict = _cache.Get<IDictionary<string, NewOrder>>(CacheKeys.BATCH_ORDERS_QUEUE_REFID_MAP);
                    IDictionary<string, OrderSummary> orderSummaryDict;
                    if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                        orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_SUMMARY);
                    else
                        orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.SIM_ORDER_SUMMARY);

                    foreach (NewOrder order in orderList)
                    {
                        if (orderSummaryDict.TryGetValue(order.OrderId, out OrderSummary orderSummary))
                        {
                            _logger.LogInformation("Updating Discount Target for Order Id: "
                                + order.OrderId + "/" + orderSummary.DscntTgt + "/" + order.DiscountTarget
                                + "/" + orderSummary.RIMaxPr + "/" + order.RefIndexMaxPrice);

                            if (order.DiscountTarget.HasValue)
                                orderSummary.DscntTgt = order.DiscountTarget;
                            if (order.RefIndexMaxPrice.HasValue)
                                orderSummary.RIMaxPr = order.RefIndexMaxPrice;

                            _logger.LogInformation("Updating Discount Target for Order Id [AFTER]: "
                                + order.OrderId + "/" + orderSummary.DscntTgt + "/" + order.DiscountTarget
                                + "/" + orderSummary.RIMaxPr + "/" + order.RefIndexMaxPrice);
                        }

                        if (!string.IsNullOrEmpty(order.Id)
                            && batchOrdersByRefIdDict.TryGetValue(order.Id, out NewOrder nOrder))
                        {
                            _logger.LogInformation("Updating Discount Target for Ref Id: "
                                + order.Id + "/" + nOrder.DiscountTarget + "/" + order.DiscountTarget
                                + "/" + nOrder.RefIndexMaxPrice + "/" + order.RefIndexMaxPrice);

                            if (order.DiscountTarget.HasValue)
                                nOrder.DiscountTarget = order.DiscountTarget;
                            if (order.RefIndexMaxPrice.HasValue)
                                nOrder.RefIndexMaxPrice = order.RefIndexMaxPrice;

                            _logger.LogInformation("Updating Discount Target for Ref Id [AFTER]: "
                                + order.Id + "/" + nOrder.DiscountTarget + "/" + order.DiscountTarget
                                + "/" + nOrder.RefIndexMaxPrice + "/" + order.RefIndexMaxPrice);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing NEW orders");
            }
        }
    }
}
