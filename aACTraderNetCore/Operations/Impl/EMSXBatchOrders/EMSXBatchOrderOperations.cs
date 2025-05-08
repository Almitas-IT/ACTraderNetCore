using aACTrader.Services.EMSX;
using aCommons;
using aCommons.Cef;
using aCommons.Trading;
using LazyCache;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace aACTrader.Operations.Impl.BatchOrders
{
    public class EMSXBatchOrderOperations
    {
        private readonly ILogger<EMSXBatchOrderOperations> _logger;
        private readonly CachingService _cache;
        private readonly EMSXOrderPublisher _emsxOrderPublisher;
        private readonly IConfiguration _configuration;
        private readonly EMSXDiscountTargetOrder _discountTargetOrder;
        private readonly EMSXRefIndexOrder _refIndexOrder;

        protected static readonly int ORDER_UPDATE_COUNT = 10;
        protected static readonly double DEFAULT_PRICE_CAP = 0.03;
        protected static readonly double DEFAULT_MIN_ORDER_UPDATE_THRESHOLD = 5; //bps
        protected static readonly double DEFAULT_MAX_ORDER_UPDATE_THRESHOLD = 50; //bps
        protected static readonly double DEFAULT_MARKET_PRICE_THRESHOLD = 0.005;
        protected static readonly double DISCOUNT_DEFAULT_MARKET_PRICE_THRESHOLD = 0.010;
        private readonly Random RandomNumberGenerator = new Random();

        public EMSXBatchOrderOperations(ILogger<EMSXBatchOrderOperations> logger
            , CachingService cache
            , EMSXOrderPublisher emsxOrderPublisher
            , IConfiguration configuration
            , EMSXDiscountTargetOrder discountTargetOrder
            , EMSXRefIndexOrder refIndexOrder)
        {
            this._logger = logger;
            this._cache = cache;
            this._emsxOrderPublisher = emsxOrderPublisher;
            this._configuration = configuration;
            this._discountTargetOrder = discountTargetOrder;
            this._refIndexOrder = refIndexOrder;
            _logger.LogInformation("Initializing EMSXBatchOrderOperations...");
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
                    IList<NewOrder> batchOrders = _cache.Get<IList<NewOrder>>(CacheKeys.EMSX_BATCH_ORDERS);
                    IDictionary<string, NewOrder> batchOrdersByRefIdDict = _cache.Get<IDictionary<string, NewOrder>>(CacheKeys.EMSX_BATCH_ORDERS_REFID_MAP);
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

                            //Populate Trader Name
                            if (string.IsNullOrEmpty(order.UserName))
                                order.UserName = "almitas-api-prod";

                            SubmitOrder(order);
                        }
                        //Process Update/Cancel Order
                        else
                        {
                            SubmitOrder(order);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing [NEW] EMSX orders");
            }
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
                _logger.LogError("Error processing the Order: " + order.Symbol + "/" + order.OrderSide + "/" + order.Id, ex);
            }

            return validOrder;
        }

        /// <summary>
        /// 
        /// </summary>
        public void ProcessRefIndexOrders()
        {
            //_logger.LogInformation("Process Ref Index Orders - STARTED");
            string environment = "PRODUCTION";
            int orderCount = 0;

            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);
            IDictionary<Int32, EMSXRouteStatus> routeStatusDict = _cache.Get<IDictionary<Int32, EMSXRouteStatus>>(CacheKeys.EMSX_ROUTE_STATUS);

            if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                environment = "SIMULATION";

            //list of orders to auto-update
            IList<NewOrder> updateOrders = new List<NewOrder>();

            foreach (KeyValuePair<Int32, EMSXRouteStatus> kvp in routeStatusDict)
            {
                EMSXRouteStatus routeStatus = kvp.Value;
                int? ordSeq = routeStatus.OrdSeq;
                string ordRefId = routeStatus.OrdRefId;
                double? previousTargetPrice = GetRoutePrice(routeStatus);

                try
                {
                    //check if the order is flagged for auto-update
                    //check if the order is reference index order
                    //check if the order is active
                    if (!string.IsNullOrEmpty(routeStatus.AutoUpdate)
                        && routeStatus.AutoUpdate.Equals("Y")
                        && !string.IsNullOrEmpty(routeStatus.RI)
                        && string.IsNullOrEmpty(routeStatus.OrdActFlag)
                        && previousTargetPrice.HasValue
                        )
                    {
                        orderCount++;

                        //get reference index price
                        SecurityPrice refIndexPrice = SecurityPriceLookupOperations.GetSecurityPrice(routeStatus.RI, priceTickerMap, securityPriceDict);
                        if (refIndexPrice != null)
                        {
                            //get live price (last/mid/bid/ask)
                            double? livePrice = null;
                            switch (routeStatus.RIPrTyp)
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
                                if (livePrice.HasValue
                                    && livePrice.GetValueOrDefault() > 0
                                    && routeStatus.RILastPr.HasValue
                                    && routeStatus.RILastPr.GetValueOrDefault() > 0)
                                {
                                    //get live price of security
                                    //this is added to make sure the bid price is less than offer price for BUY orders
                                    //and offer price is more than bid price for SELL orders
                                    //string symbol = !string.IsNullOrEmpty(routeStatus.ALMSym) ? routeStatus.ALMSym : routeStatus.BBGSym;
                                    string symbol = routeStatus.Ticker;
                                    SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(symbol, priceTickerMap, securityPriceDict);

                                    //applies beta on price change
                                    if ("Pct".Equals(routeStatus.RIBetaAdjTyp, StringComparison.CurrentCultureIgnoreCase))
                                        _refIndexOrder.ProcessRefIndexBetaOrder(routeStatus, livePrice, previousTargetPrice, securityPrice);
                                    //applies delta on underlying price
                                    else if ("Delta".Equals(routeStatus.RIBetaAdjTyp, StringComparison.CurrentCultureIgnoreCase))
                                        _refIndexOrder.ProcessRefIndexDeltaOrder(routeStatus, livePrice, previousTargetPrice, securityPrice);
                                    //applies abs diff on underlying price
                                    else if ("Abs".Equals(routeStatus.RIBetaAdjTyp, StringComparison.CurrentCultureIgnoreCase))
                                        _refIndexOrder.ProcessRefIndexAbsOrder(routeStatus, livePrice, previousTargetPrice, securityPrice);

                                    //_logger.LogInformation("Processing Order: "
                                    //    + routeStatus.OrderId + "/"
                                    //    + routeStatus.OrderStatus + "/"
                                    //    + routeStatus.ALMOrderStatus + "/"
                                    //    + routeStatus.OrderPrice + "/"
                                    //    + routeStatus.RefIndexTargetPrice);

                                    //check if order is to be updated
                                    if (CheckForAutoUpdate(routeStatus) && CheckForMarketPriceThreshold(routeStatus))
                                        CreateUpdateRoute(routeStatus, environment, updateOrders);
                                }
                            }
                            else
                            {
                                _logger.LogInformation("Ref Index Bid Price > Ask Price: "
                                    + routeStatus.Ticker + "/"
                                    + routeStatus.OrdSeq + "/"
                                    + routeStatus.OrdRefId + "/"
                                    + routeStatus.RI + "/"
                                    + refIndexPrice.BidPrc + "/"
                                    + refIndexPrice.AskPrc + "/"
                                    + routeStatus.BidAskPrFlag);

                                routeStatus.BidAskPrFlag++;
                                if (routeStatus.BidAskPrFlag > 5)
                                {
                                    _logger.LogInformation("Canceling Order (Bid > Ask): "
                                        + routeStatus.Ticker + "/"
                                        + routeStatus.OrdSeq + "/"
                                        + routeStatus.OrdRefId + "/"
                                        + routeStatus.RI + "/"
                                        + refIndexPrice.BidPrc + "/"
                                        + refIndexPrice.AskPrc + "/"
                                        + routeStatus.BidAskPrFlag);

                                    CancelOrder(routeStatus, environment, updateOrders);
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing Ref Index order: " + ordSeq + "/" + ordRefId);
                }
            }

            //submit updated orders
            if (updateOrders.Count > 0)
            {
                if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                    ProcessNewOrders(updateOrders);
                else if (_configuration["ConnectionStrings:ENV"].Equals("DEV") && _configuration["ConnectionStrings:EMSX_UAT_ON"].Equals("Y"))
                    ProcessNewOrders(updateOrders);
            }

            _logger.LogInformation("Processed Ref Index Orders: " + orderCount);
        }

        /// <summary>
        /// 
        /// </summary>
        public void ProcessDiscountOrders()
        {
            //_logger.LogInformation("Process EMSX Discount Orders - STARTED");
            string environment = "PRODUCTION";
            int orderCount = 0;

            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
            IDictionary<Int32, EMSXRouteStatus> routeStatusDict = _cache.Get<IDictionary<Int32, EMSXRouteStatus>>(CacheKeys.EMSX_ROUTE_STATUS);
            IDictionary<Int32, EMSXOrderStatus> orderStatusDict = _cache.Get<IDictionary<Int32, EMSXOrderStatus>>(CacheKeys.EMSX_ORDER_STATUS);

            if (_configuration["ConnectionStrings:ENV"].Equals("DEV"))
                environment = "SIMULATION";

            //list of orders to auto-update
            IList<NewOrder> updateOrders = new List<NewOrder>();

            foreach (EMSXRouteStatus routeStatus in routeStatusDict.Values)
            {
                int? ordSeq = routeStatus.OrdSeq;
                string ordRefId = routeStatus.OrdRefId;
                double? previousTargetPrice = GetRoutePrice(routeStatus);

                try
                {
                    //check if the order is flagged for auto-update
                    //check if the order is discount order
                    //check if the order is active
                    if (!string.IsNullOrEmpty(routeStatus.AutoUpdate)
                        && routeStatus.AutoUpdate.Equals("Y")
                        && routeStatus.DscntTgt.HasValue
                        && string.IsNullOrEmpty(routeStatus.OrdActFlag)
                        && previousTargetPrice.HasValue
                        )
                    {
                        orderCount++;

                        //get live price of security
                        string symbol = routeStatus.Ticker;
                        SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(symbol, priceTickerMap, securityPriceDict);

                        //process discount order
                        _discountTargetOrder.ProcessDiscountOrder(routeStatus, fundForecastDict, previousTargetPrice, securityPrice);

                        //check if order is to be updated
                        if (CheckForAutoUpdate(routeStatus) && CheckForMarketPriceThreshold(routeStatus))
                            CreateUpdateRoute(routeStatus, environment, updateOrders);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing Order for Ref Index/Discount Target change: " + ordSeq + "/" + ordRefId);
                }
            }

            //submit updated orders
            if (updateOrders.Count > 0)
            {
                if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                    ProcessNewOrders(updateOrders);
                else if (_configuration["ConnectionStrings:ENV"].Equals("DEV") && _configuration["ConnectionStrings:EMSX_UAT_ON"].Equals("Y"))
                    ProcessNewOrders(updateOrders);
            }
            //_logger.LogInformation("Processed Discount Orders: " + orderCount);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="routeStatus"></param>
        /// <returns></returns>
        private bool CheckForMarketPriceThreshold(EMSXRouteStatus routeStatus)
        {
            bool marketPriceThresholdFlag = false;

            try
            {
                double? currentOrderPrice = GetRoutePrice(routeStatus);
                double newOrderPrice = routeStatus.NewOrdPr.GetValueOrDefault();
                double priceChange = (newOrderPrice / currentOrderPrice.GetValueOrDefault()) - 1.0;

                if (routeStatus.MktPrThld.HasValue)
                {
                    if (Math.Abs(routeStatus.MktPrSprd.GetValueOrDefault()) > routeStatus.MktPrThld.GetValueOrDefault())
                    {
                        if (Math.Abs(priceChange * 10000.0) > DEFAULT_MAX_ORDER_UPDATE_THRESHOLD)
                        {
                            marketPriceThresholdFlag = true;
                            _logger.LogInformation("[Outside Price Threshold && Price Change > 50 bps]: "
                                + routeStatus.Ticker + "/"
                                + routeStatus.OrdSeq + "/"
                                + routeStatus.OrdRefId + "/"
                                + currentOrderPrice + "/"
                                + routeStatus.NewOrdPr + "/"
                                + routeStatus.BidPr + "/"
                                + routeStatus.AskPr + "/"
                                + routeStatus.MktPrSprd + "/"
                                + routeStatus.MktPrThld + "/"
                                + priceChange);
                        }
                    }
                    else
                    {
                        if (Math.Abs(priceChange * 10000.0) > DEFAULT_MIN_ORDER_UPDATE_THRESHOLD)
                        {
                            marketPriceThresholdFlag = true;
                            _logger.LogInformation("[Within Price Threshold && Price Change > 5 bps]: "
                                + routeStatus.Ticker + "/"
                                + routeStatus.OrdSeq + "/"
                                + routeStatus.OrdRefId + "/"
                                + currentOrderPrice + "/"
                                + routeStatus.NewOrdPr + "/"
                                + routeStatus.BidPr + "/"
                                + routeStatus.AskPr + "/"
                                + routeStatus.MktPrSprd + "/"
                                + routeStatus.MktPrThld + "/"
                                + priceChange);
                        }
                    }
                }

                if (!marketPriceThresholdFlag)
                {
                    _logger.LogInformation("[Price Threshold Check]: "
                                + routeStatus.Ticker + "/"
                                + routeStatus.OrdSeq + "/"
                                + routeStatus.OrdRefId + "/"
                                + currentOrderPrice + "/"
                                + routeStatus.NewOrdPr + "/"
                                + routeStatus.BidPr + "/"
                                + routeStatus.AskPr + "/"
                                + routeStatus.MktPrSprd + "/"
                                + routeStatus.MktPrThld + "/"
                                + priceChange);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating Market Price Spread for Order: " + routeStatus.OrdRefId + "/" + routeStatus.RITgtPr);
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
        /// New Price (based on Estimated Nav and Discount Target)
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
                    double? nav = _discountTargetOrder.GetEstimatedNav(order.EstNavType, fundForecast);
                    if (nav.HasValue)
                    {
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
                _logger.LogError(ex, "Error calculating EMSX Order Price using Ref Index Price");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="order"></param>
        private void SubmitOrderSim(NewOrder order)
        {
            try
            {
                _emsxOrderPublisher.PublishMessage(order);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error submitting EMSX New/Update orders [Simulation Environment]");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="order"></param>
        private void SubmitOrder(NewOrder order)
        {
            try
            {
                _emsxOrderPublisher.PublishMessage(order);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error submitting EMSX New/Update orders [Production Environment]");
            }
        }

        /// <summary>
        /// Check if the Order is ready to be replaced (Price/Qty change)
        /// Order can be replaced for max of 10 times (for now)
        /// </summary>
        /// <param name="routeStatus"></param>
        /// <returns></returns>
        protected bool CheckForAutoUpdate(EMSXRouteStatus routeStatus)
        {
            bool result = false;
            try
            {
                if ((routeStatus.Status.Equals("PARTFILL")
                    || routeStatus.Status.Equals("PARTFILLED")
                    //|| routeStatus.Status.Equals("SENT")
                    || routeStatus.Status.Equals("WORKING"))
                && string.IsNullOrEmpty(routeStatus.ALMStatus))
                {
                    result = true;
                }
                else
                {
                    _logger.LogInformation("CheckForAutoUpdate - "
                        + routeStatus.Ticker + "/"
                        + routeStatus.OrdSeq + "/"
                        + routeStatus.OrdRefId + "/"
                        + routeStatus.Status);
                }
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error checking EMSX Order status");
                return result;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="routeStatus"></param>
        /// <param name="emsxOrderStatus"></param>
        /// <returns></returns>
        protected bool CheckForOrderStatus(EMSXRouteStatus routeStatus, EMSXOrderStatus emsxOrderStatus)
        {
            bool result = false;
            try
            {
                if ((routeStatus.Status.Equals("PARTFILL")
                    || routeStatus.Status.Equals("PARTFILLED")
                    //|| routeStatus.Status.Equals("SENT")
                    || routeStatus.Status.Equals("WORKING"))
                && string.IsNullOrEmpty(routeStatus.ALMStatus))
                {
                    result = true;
                }
                else
                {
                    _logger.LogInformation("CheckForAutoUpdate - "
                        + routeStatus.Ticker + "/"
                        + routeStatus.OrdSeq + "/"
                        + routeStatus.OrdRefId + "/"
                        + routeStatus.Status);
                }
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error checking EMSX Order status");
                return result;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="orderRoute"></param>
        /// <param name="updateOrders"></param>
        protected void CancelOrder(EMSXRouteStatus orderRoute
            , string environment, IList<NewOrder> updateOrders)
        {
            NewOrder cancelOrder = new NewOrder();

            string orderId = orderRoute.OrdSeq.GetValueOrDefault().ToString();
            cancelOrder.ActionType = "Cancel";
            cancelOrder.Id = orderRoute.OrdRefId;
            cancelOrder.OrderId = orderId;
            cancelOrder.OrderSeq = orderRoute.OrdSeq;
            cancelOrder.RouteId = orderRoute.RouteId;
            cancelOrder.Symbol = orderRoute.Ticker;
            cancelOrder.OrderSide = orderRoute.OrdSide;
            cancelOrder.OrderType = orderRoute.OrdType;
            cancelOrder.OrderStatus = orderRoute.Status;
            cancelOrder.UserName = orderRoute.Trader;
            cancelOrder.Environment = environment;
            cancelOrder.OrderQty = orderRoute.Amt;
            cancelOrder.OrderPrice = orderRoute.RoutePrc;
            cancelOrder.OrderLimitPrice = orderRoute.LimitPrc;
            cancelOrder.OrderStopPrice = orderRoute.StopPrc;
            cancelOrder.Destination = orderRoute.Bkr;
            cancelOrder.BrokerStrategy = orderRoute.StratStyle;

            _logger.LogInformation("Canceling Order: " +
                string.Join("/",
                orderRoute.Ticker,
                orderRoute.OrdSeq,
                orderRoute.RouteId,
                orderRoute.OrdSide,
                orderRoute.OrdType,
                orderRoute.Status));

            updateOrders.Add(cancelOrder);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="routeStatus"></param>
        /// <param name="environment"></param>
        /// <param name="updateOrders"></param>
        protected void CreateUpdateOrder(EMSXRouteStatus routeStatus
            , string environment, IList<NewOrder> updateOrders)
        {
            try
            {
                double? routePrice = GetRoutePrice(routeStatus);
                double currentOrderPrice = routePrice.GetValueOrDefault();
                double newOrderPrice = routeStatus.NewOrdPr.GetValueOrDefault();

                /////////////////////TODO: Added for TESTING only
                if (_configuration["ConnectionStrings:ENV"].Equals("DEV") && _configuration["ConnectionStrings:EMSX_UAT_ON"].Equals("Y"))
                {
                    routeStatus.RITgtPr *= (1.0 + RandomNumberGenerator.NextDouble() / 10.0);
                    newOrderPrice = Math.Round(routeStatus.RITgtPr.GetValueOrDefault(), 2);
                }

                if (newOrderPrice > 0.0 && (currentOrderPrice != newOrderPrice))
                {
                    NewOrder updateOrder = new NewOrder();
                    updateOrder.ActionType = "UpdateOrder";
                    updateOrder.Id = routeStatus.OrdRefId;
                    updateOrder.OrderSeq = routeStatus.OrdSeq;
                    updateOrder.RouteId = routeStatus.RouteId;
                    updateOrder.Symbol = routeStatus.Ticker;
                    updateOrder.OrderSide = routeStatus.OrdSide;
                    updateOrder.OrderType = routeStatus.OrdType;
                    updateOrder.OrderPrice = GetRoutePrice(routeStatus);
                    updateOrder.OrderLimitPrice = routeStatus.LimitPrc;
                    updateOrder.OrderStopPrice = routeStatus.StopPrc;
                    updateOrder.OrigOrderPrice = routeStatus.OrdOrigPr;
                    updateOrder.OrderQty = routeStatus.Amt;
                    updateOrder.OrderStatus = routeStatus.Status;
                    updateOrder.NewOrderPrice = newOrderPrice; // NEW Order Price
                    updateOrder.NewOrderQty = routeStatus.Amt; // NEW Order Qty
                    updateOrder.UserName = routeStatus.Trader;
                    updateOrder.Environment = environment;
                    updateOrder.Destination = routeStatus.Bkr;
                    updateOrder.BrokerStrategy = routeStatus.StratType;
                    updateOrder.AlgoParameters = routeStatus.AlgoParams;
                    updateOrder.AlgoParams = routeStatus.AlgoParamsList;
                    updateOrder.Sedol = routeStatus.Sedol;

                    //Ref Index and Discount Target Fields
                    updateOrder.RefIndex = routeStatus.RI;
                    updateOrder.RefIndexPriceType = routeStatus.RIPrTyp;
                    updateOrder.RefIndexPriceBeta = routeStatus.RIBeta;
                    updateOrder.RefIndexBetaAdjType = routeStatus.RIBetaAdjTyp;
                    updateOrder.RefIndexPriceBetaShiftInd = routeStatus.RIPrBetaShiftInd;
                    updateOrder.RefIndexPriceCap = routeStatus.RIPrCap;
                    updateOrder.RefIndexPriceCapShiftInd = routeStatus.RIPrCapShiftInd;
                    updateOrder.RefIndexMaxPrice = routeStatus.RIMaxPr;
                    updateOrder.DiscountTarget = routeStatus.DscntTgt;
                    updateOrder.AutoUpdate = routeStatus.AutoUpdate;
                    updateOrder.AutoUpdateThreshold = routeStatus.AutoUpdateThld;
                    updateOrder.MarketPriceThreshold = routeStatus.MktPrThld;
                    updateOrder.MarketPriceField = routeStatus.MktPrFld;

                    //Update ALM Order Status
                    routeStatus.ALMStatus = "Order Replace Sent";

                    _logger.LogInformation("Updating Order: " +
                        string.Join("/"
                        , routeStatus.Ticker
                        , routeStatus.OrdSeq
                        , routeStatus.RouteId
                        , routeStatus.OrdRefId
                        , routeStatus.Status
                        , routeStatus.RoutePrc
                        , routeStatus.NewOrdPr));

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
        /// <param name="routeStatus"></param>
        /// <param name="environment"></param>
        /// <param name="updateOrders"></param>
        protected void CreateUpdateRoute(EMSXRouteStatus routeStatus
            , string environment, IList<NewOrder> updateOrders)
        {
            try
            {
                double? routePrice = GetRoutePrice(routeStatus);
                double currentOrderPrice = routePrice.GetValueOrDefault();
                double newOrderPrice = routeStatus.NewOrdPr.GetValueOrDefault();

                /////////////////////TODO: Added for TESTING only
                if (_configuration["ConnectionStrings:ENV"].Equals("DEV") && _configuration["ConnectionStrings:EMSX_UAT_ON"].Equals("Y"))
                {
                    routeStatus.RITgtPr *= (1.0 + RandomNumberGenerator.NextDouble() / 10.0);
                    newOrderPrice = Math.Round(routeStatus.RITgtPr.GetValueOrDefault(), 2);
                }

                if (newOrderPrice > 0.0 && (currentOrderPrice != newOrderPrice))
                {
                    NewOrder updateOrder = new NewOrder();
                    updateOrder.ActionType = "UpdateRoute";
                    updateOrder.Id = routeStatus.OrdRefId;
                    updateOrder.OrderSeq = routeStatus.OrdSeq;
                    updateOrder.RouteId = routeStatus.RouteId;
                    updateOrder.Symbol = routeStatus.Ticker;
                    updateOrder.OrderSide = routeStatus.OrdSide;
                    updateOrder.OrderType = routeStatus.OrdType;
                    updateOrder.OrderPrice = GetRoutePrice(routeStatus);
                    updateOrder.OrderLimitPrice = routeStatus.LimitPrc;
                    updateOrder.OrderStopPrice = routeStatus.StopPrc;
                    updateOrder.OrigOrderPrice = routeStatus.OrdOrigPr;
                    updateOrder.OrderQty = routeStatus.Amt;
                    updateOrder.OrderStatus = routeStatus.Status;
                    updateOrder.NewOrderPrice = newOrderPrice; // NEW Order Price
                    updateOrder.NewOrderQty = routeStatus.Amt; // NEW Order Qty
                    updateOrder.UserName = routeStatus.Trader;
                    updateOrder.Environment = environment;
                    updateOrder.Destination = routeStatus.Bkr;
                    updateOrder.BrokerStrategy = routeStatus.StratType;
                    updateOrder.AlgoParameters = routeStatus.AlgoParams;
                    updateOrder.AlgoParams = routeStatus.AlgoParamsList;
                    updateOrder.Sedol = routeStatus.Sedol;

                    //Ref Index and Discount Target Fields
                    updateOrder.RefIndex = routeStatus.RI;
                    updateOrder.RefIndexPriceType = routeStatus.RIPrTyp;
                    updateOrder.RefIndexPriceBeta = routeStatus.RIBeta;
                    updateOrder.RefIndexBetaAdjType = routeStatus.RIBetaAdjTyp;
                    updateOrder.RefIndexPriceBetaShiftInd = routeStatus.RIPrBetaShiftInd;
                    updateOrder.RefIndexPriceCap = routeStatus.RIPrCap;
                    updateOrder.RefIndexPriceCapShiftInd = routeStatus.RIPrCapShiftInd;
                    updateOrder.RefIndexMaxPrice = routeStatus.RIMaxPr;
                    updateOrder.DiscountTarget = routeStatus.DscntTgt;
                    updateOrder.AutoUpdate = routeStatus.AutoUpdate;
                    updateOrder.AutoUpdateThreshold = routeStatus.AutoUpdateThld;
                    updateOrder.MarketPriceThreshold = routeStatus.MktPrThld;
                    updateOrder.MarketPriceField = routeStatus.MktPrFld;

                    //Update ALM Order Status
                    routeStatus.ALMStatus = "Route Replace Sent";

                    _logger.LogInformation("Updating Route: " +
                        string.Join("/"
                        , routeStatus.Ticker
                        , routeStatus.OrdSeq
                        , routeStatus.RouteId
                        , routeStatus.OrdRefId
                        , routeStatus.Status
                        , routeStatus.RoutePrc
                        , routeStatus.NewOrdPr));

                    updateOrders.Add(updateOrder);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error creating Update Order");
            }
        }

        /// <summary>
        /// Cancels API Orders
        /// </summary>
        public void CancelAPIOrders()
        {
            IList<NewOrder> cancelOrders = new List<NewOrder>();
            IDictionary<Int32, EMSXRouteStatus> routeDict = _cache.Get<IDictionary<Int32, EMSXRouteStatus>>(CacheKeys.EMSX_ROUTE_STATUS);
            string environment = "PRODUCTION";

            if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                environment = "SIMULATION";

            foreach (EMSXRouteStatus orderRoute in routeDict.Values.ToList<EMSXRouteStatus>())
            {
                try
                {
                    if (!string.IsNullOrEmpty(orderRoute.OrdRefId))
                        CancelOrder(orderRoute, environment, cancelOrders);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error canceling Order: " + orderRoute.OrdSeq + "/" + orderRoute.RouteId + "/" + orderRoute.Trader + "/" + orderRoute.Ticker);
                }
            }

            //Cancel API Orders
            if (cancelOrders.Count > 0)
            {
                if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                    ProcessNewOrders(cancelOrders);
                else if (_configuration["ConnectionStrings:ENV"].Equals("DEV") && _configuration["ConnectionStrings:EMSX_UAT_ON"].Equals("Y"))
                    ProcessNewOrders(cancelOrders);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="order"></param>
        /// <returns></returns>
        private bool ValidateOpenOrder(NewOrder order)
        {
            bool validOrder = true;
            try
            {
                if (!order.OrderPrice.HasValue || order.OrderPrice.GetValueOrDefault() == 0)
                    validOrder = false;
                if (!order.OrderQty.HasValue || order.OrderQty.GetValueOrDefault() == 0)
                    validOrder = false;
            }
            catch (Exception)
            {
            }
            return validOrder;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="routeStatus"></param>
        /// <returns></returns>
        private double? GetRoutePrice(EMSXRouteStatus routeStatus)
        {
            double? routePrice = null;
            if (routeStatus.LimitPrc.HasValue && routeStatus.LimitPrc.GetValueOrDefault() > 0)
                routePrice = routeStatus.LimitPrc;
            else if (routeStatus.StopPrc.HasValue && routeStatus.StopPrc.GetValueOrDefault() > 0)
                routePrice = routeStatus.StopPrc;
            else if (routeStatus.RoutePrc.HasValue && routeStatus.RoutePrc.GetValueOrDefault() > 0)
                routePrice = routeStatus.RoutePrc;
            return routePrice;
        }
    }
}