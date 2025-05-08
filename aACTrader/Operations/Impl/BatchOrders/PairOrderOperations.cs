using aACTrader.Services.Neovest;
using aCommons;
using aCommons.Trading;
using LazyCache;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using ConfigurationManager = System.Configuration.ConfigurationManager;

namespace aACTrader.Operations.Impl.BatchOrders
{
    public class PairOrderOperations
    {
        private readonly ILogger<PairOrderOperations> _logger;
        private readonly CachingService _cache;
        private readonly IConfiguration _configuration;
        private readonly SimNeovestOrderPublisher _simNeovestOrderPublisher;
        private readonly NeovestOrderPublisher _neovestOrderPublisher;

        protected static readonly string ENV = ConfigurationManager.AppSettings["ENV"];

        public PairOrderOperations(ILogger<PairOrderOperations> logger
            , CachingService cache
            , IConfiguration configuration
            , SimNeovestOrderPublisher simNeovestOrderPublisher
            , NeovestOrderPublisher neovestOrderPublisher)
        {
            this._logger = logger;
            this._cache = cache;
            this._configuration = configuration;
            this._simNeovestOrderPublisher = simNeovestOrderPublisher;
            this._neovestOrderPublisher = neovestOrderPublisher;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="pairOrders"></param>
        public void ProcessNewOrders(IList<PairOrderDetail> pairOrders)
        {
            try
            {
                if (pairOrders != null && pairOrders.Count > 0)
                {
                    IDictionary<string, PairOrder> batchOrders = _cache.Get<IDictionary<string, PairOrder>>(CacheKeys.PAIR_TRADE_BATCH_ORDERS);

                    PairOrder pairOrder = new PairOrder();
                    foreach (PairOrderDetail order in pairOrders)
                    {
                        if ("B".Equals(order.OrderSide, StringComparison.CurrentCultureIgnoreCase)
                            || "BUY".Equals(order.OrderSide, StringComparison.CurrentCultureIgnoreCase))
                            pairOrder.BuyOrder = order;
                        else if ("S".Equals(order.OrderSide, StringComparison.CurrentCultureIgnoreCase)
                            || "SELL".Equals(order.OrderSide, StringComparison.CurrentCultureIgnoreCase)
                            )
                            pairOrder.SellOrder = order;

                        //Populate Parent Id
                        pairOrder.ParentId = order.ParentId;
                        order.OrderDetail.MainParentId = order.ParentId;
                        order.OrderDetail.ParentId = order.Id;
                        order.OrderDetail.OrderRefId = order.Id + "_" + order.SeqId;
                    }

                    //Add to Batch Orders List
                    if (!batchOrders.ContainsKey(pairOrder.ParentId))
                        batchOrders.Add(pairOrder.ParentId, pairOrder);
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
        public void ProcessOrders()
        {
            IDictionary<string, PairOrder> orders = _cache.Get<IDictionary<string, PairOrder>>(CacheKeys.PAIR_TRADE_BATCH_ORDERS);

            IDictionary<string, OrderSummary> orderSummaryDict;
            if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_SUMMARY);
            else
                orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.SIM_ORDER_SUMMARY);

            IDictionary<string, string> refIdOrderDict = _cache.Get<IDictionary<string, string>>(CacheKeys.REFID_ORDERS_MAP);

            foreach (KeyValuePair<string, PairOrder> kvp in orders)
            {
                PairOrder order = kvp.Value;
                string parentId = order.ParentId;

                //Check Order Status
                CheckOrderStatus(order, orderSummaryDict, refIdOrderDict);

                if (string.IsNullOrEmpty(order.OrderActiveFlag) || "N".Equals(order.OrderActiveFlag))
                {
                    _logger.LogInformation("Processing Parent Id: " + parentId);

                    if (!order.Tradable)
                        CheckRatioLevel(order);

                    if (order.Tradable)
                    {
                        PairOrderDetailExt buyOrderDetailExt = order.BuyOrder.OrderDetail;
                        PairOrderDetailExt sellOrderDetailExt = order.SellOrder.OrderDetail;
                        string pairInitiateLeg = order.BuyOrder.PairInitiateLeg;

                        if ("BUY first".Equals(pairInitiateLeg, StringComparison.CurrentCultureIgnoreCase))
                        {
                            ProcessBuyLeg(order, orderSummaryDict, refIdOrderDict);
                            if ("N".Equals(buyOrderDetailExt.OrderActiveFlag))
                                ProcessSellLeg(order, orderSummaryDict, refIdOrderDict);
                        }
                        else if ("SELL first".Equals(pairInitiateLeg, StringComparison.CurrentCultureIgnoreCase))
                        {
                            ProcessSellLeg(order, orderSummaryDict, refIdOrderDict);
                            if ("N".Equals(sellOrderDetailExt.OrderActiveFlag))
                                ProcessBuyLeg(order, orderSummaryDict, refIdOrderDict);
                        }

                        if ("N".Equals(buyOrderDetailExt.OrderActiveFlag) && "N".Equals(sellOrderDetailExt.OrderActiveFlag))
                        {
                            order.OrderActiveFlag = "N";
                            CalculateExecutedRatioLevel(order);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="order"></param>
        private void CheckRatioLevel(PairOrder order)
        {
            PairOrderDetail buyOrder = order.BuyOrder;
            PairOrderDetail sellOrder = order.SellOrder;

            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);

            SecurityPrice buyOrderPrice = SecurityPriceLookupOperations.GetSecurityPrice(buyOrder.Symbol, priceTickerMap, securityPriceDict);
            SecurityPrice sellOrderPrice = SecurityPriceLookupOperations.GetSecurityPrice(sellOrder.Symbol, priceTickerMap, securityPriceDict);

            if (buyOrderPrice != null && sellOrderPrice != null)
            {
                string pairRatioSetup = buyOrder.PairRatioSetup;
                string pairInitiateLeg = order.BuyOrder.PairInitiateLeg;
                string pairSpreadOperator = buyOrder.PairSpreadOper;
                double? pairRatio = buyOrder.PairRatio;

                if (pairRatioSetup != null)
                {
                    double ratioLast = 0;
                    double ratioMarket = 0;
                    double ratioPassive = 0;
                    double distance = 0;
                    bool tradable = false;

                    if ("SELL/BUY".Equals(pairRatioSetup, StringComparison.CurrentCultureIgnoreCase))
                    {
                        if (buyOrderPrice.LastPrc.HasValue && buyOrderPrice.LastPrc.GetValueOrDefault() > 0)
                            ratioLast = sellOrderPrice.LastPrc.GetValueOrDefault() / buyOrderPrice.LastPrc.GetValueOrDefault();
                        if (buyOrderPrice.AskPrc.HasValue && buyOrderPrice.AskPrc.GetValueOrDefault() > 0)
                            ratioMarket = sellOrderPrice.BidPrc.GetValueOrDefault() / buyOrderPrice.AskPrc.GetValueOrDefault();
                        if (buyOrderPrice.BidPrc.HasValue && buyOrderPrice.BidPrc.GetValueOrDefault() > 0)
                            ratioPassive = sellOrderPrice.AskPrc.GetValueOrDefault() / buyOrderPrice.BidPrc.GetValueOrDefault();

                        distance = ratioMarket - ratioLast;
                        tradable = false;

                        if ("BUY first".Equals(pairInitiateLeg, StringComparison.CurrentCultureIgnoreCase))
                        {
                            buyOrder.OrderDetail.OrderPriceType = "ASK";
                            sellOrder.OrderDetail.OrderPriceType = "BUY*RATIO";
                        }
                        else if ("SELL first".Equals(pairInitiateLeg, StringComparison.CurrentCultureIgnoreCase))
                        {
                            sellOrder.OrderDetail.OrderPriceType = "BID";
                            buyOrder.OrderDetail.OrderPriceType = "SELL/RATIO";
                        }
                    }
                    else if ("BUY/SELL".Equals(pairRatioSetup, StringComparison.CurrentCultureIgnoreCase))
                    {
                        if (sellOrderPrice.LastPrc.HasValue && sellOrderPrice.LastPrc.GetValueOrDefault() > 0)
                            ratioLast = buyOrderPrice.LastPrc.GetValueOrDefault() / sellOrderPrice.LastPrc.GetValueOrDefault();
                        if (sellOrderPrice.BidPrc.HasValue && sellOrderPrice.BidPrc.GetValueOrDefault() > 0)
                            ratioMarket = buyOrderPrice.AskPrc.GetValueOrDefault() / sellOrderPrice.BidPrc.GetValueOrDefault();
                        if (sellOrderPrice.AskPrc.HasValue && sellOrderPrice.AskPrc.GetValueOrDefault() > 0)
                            ratioPassive = buyOrderPrice.BidPrc.GetValueOrDefault() / sellOrderPrice.AskPrc.GetValueOrDefault();

                        distance = ratioMarket - ratioLast;
                        tradable = false;

                        if ("BUY first".Equals(pairInitiateLeg, StringComparison.CurrentCultureIgnoreCase))
                        {
                            buyOrder.OrderDetail.OrderPriceType = "ASK";
                            sellOrder.OrderDetail.OrderPriceType = "BUY/RATIO";
                        }
                        else if ("SELL first".Equals(pairInitiateLeg, StringComparison.CurrentCultureIgnoreCase))
                        {
                            sellOrder.OrderDetail.OrderPriceType = "BID";
                            buyOrder.OrderDetail.OrderPriceType = "SELL*RATIO";
                        }
                    }

                    if (pairRatio.HasValue)
                    {
                        if (">=".Equals(pairSpreadOperator, StringComparison.CurrentCultureIgnoreCase))
                        {
                            if (ratioMarket >= pairRatio.GetValueOrDefault())
                                tradable = true;
                        }
                        else if ("<=".Equals(pairSpreadOperator, StringComparison.CurrentCultureIgnoreCase))
                        {
                            if (ratioMarket <= pairRatio.GetValueOrDefault())
                                tradable = true;
                        }
                    }

                    order.Tradable = tradable;

                    //BUY Order
                    buyOrder.OrderDetail.LastPrc = buyOrderPrice.LastPrc;
                    buyOrder.OrderDetail.BidPrc = buyOrderPrice.BidPrc;
                    buyOrder.OrderDetail.AskPrc = buyOrderPrice.AskPrc;
                    buyOrder.OrderDetail.Vol = buyOrderPrice.Vol;
                    buyOrder.OrderDetail.BidSz = buyOrderPrice.BidSz;
                    buyOrder.OrderDetail.AskSz = buyOrderPrice.AskSz;
                    buyOrder.OrderDetail.RatioLast = ratioLast;
                    buyOrder.OrderDetail.RatioMarket = ratioMarket;
                    buyOrder.OrderDetail.RatioPassive = ratioPassive;
                    buyOrder.OrderDetail.Distance = distance;
                    buyOrder.OrderDetail.Tradable = tradable;

                    //SELL Order
                    sellOrder.OrderDetail.LastPrc = sellOrderPrice.LastPrc;
                    sellOrder.OrderDetail.BidPrc = sellOrderPrice.BidPrc;
                    sellOrder.OrderDetail.AskPrc = sellOrderPrice.AskPrc;
                    sellOrder.OrderDetail.Vol = sellOrderPrice.Vol;
                    sellOrder.OrderDetail.BidSz = sellOrderPrice.BidSz;
                    sellOrder.OrderDetail.AskSz = sellOrderPrice.AskSz;
                    sellOrder.OrderDetail.RatioLast = ratioLast;
                    sellOrder.OrderDetail.RatioMarket = ratioMarket;
                    sellOrder.OrderDetail.RatioPassive = ratioPassive;
                    sellOrder.OrderDetail.Distance = distance;
                    sellOrder.OrderDetail.Tradable = tradable;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="order"></param>
        private void CalculateExecutedRatioLevel(PairOrder order)
        {
            try
            {
                PairOrderDetailExt buyOrderExt = order.BuyOrder.OrderDetail;
                PairOrderDetailExt sellOrderExt = order.SellOrder.OrderDetail;

                double? ratio = null;
                string pairRatioSetup = order.BuyOrder.PairRatioSetup;
                if ("SELL/BUY".Equals(pairRatioSetup, StringComparison.CurrentCultureIgnoreCase))
                {
                    if (buyOrderExt.AvgTradedPrice.HasValue && buyOrderExt.AvgTradedPrice.GetValueOrDefault() > 0)
                        ratio = sellOrderExt.AvgTradedPrice.GetValueOrDefault() / buyOrderExt.AvgTradedPrice.GetValueOrDefault();
                }
                else if ("BUY/SELL".Equals(pairRatioSetup, StringComparison.CurrentCultureIgnoreCase))
                {
                    if (sellOrderExt.AvgTradedPrice.HasValue && sellOrderExt.AvgTradedPrice.GetValueOrDefault() > 0)
                        ratio = buyOrderExt.AvgTradedPrice.GetValueOrDefault() / sellOrderExt.AvgTradedPrice.GetValueOrDefault();
                }

                buyOrderExt.RatioFilled = ratio;
                sellOrderExt.RatioFilled = ratio;
            }
            catch (Exception)
            {
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="order"></param>
        /// <param name="orderExt"></param>
        /// <returns></returns>
        private NewOrder CreateNewOrder(PairOrderDetail order, PairOrderDetailExt orderExt)
        {
            NewOrder newOrder = new NewOrder();

            newOrder.ActionType = "New";
            newOrder.Symbol = order.Symbol;
            newOrder.OrderSide = order.OrderSide;
            newOrder.AccountName = order.AccountNumber;
            newOrder.Destination = order.Destination;
            newOrder.BrokerStrategy = order.BrokerStrategy;
            newOrder.OrderQty = order.OrderQty;
            newOrder.UserName = order.UserName;
            newOrder.Environment = order.Environment;
            newOrder.AlgoParameters = order.AlgoParameters;
            newOrder.AlgoParams = order.AlgoParams;

            // Derived
            newOrder.Id = orderExt.OrderRefId;
            newOrder.OrderType = orderExt.OrderType;
            newOrder.OrderPrice = orderExt.DerivedOrderPrice;
            newOrder.OrderLimitPrice = orderExt.DerivedOrderPrice;
            newOrder.OrderStopPrice = orderExt.DerivedOrderPrice;

            return newOrder;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="order"></param>
        private void ProcessSellLeg(PairOrder order
            , IDictionary<string, OrderSummary> orderSummaryDict
            , IDictionary<string, string> refIdOrderDict)
        {
            PairOrderDetail sellOrder = order.SellOrder;
            PairOrderDetailExt sellOrderExt = order.SellOrder.OrderDetail;
            PairOrderDetailExt buyOrderExt = order.BuyOrder.OrderDetail;
            string orderStatus = sellOrderExt.OrderStatus;

            if (string.IsNullOrEmpty(orderStatus))
                CheckOrderLegStatus(sellOrder, orderSummaryDict, refIdOrderDict);

            if (string.IsNullOrEmpty(orderStatus)
                && string.IsNullOrEmpty(sellOrderExt.OrderActiveFlag)
                && !sellOrderExt.TradedQty.HasValue)
            {
                CalculateOrderPrice(sellOrder, sellOrderExt, buyOrderExt);
                NewOrder newOrder = CreateNewOrder(sellOrder, sellOrderExt);
                sellOrderExt.ALMOrderStatus = "Submitted";
                sellOrderExt.OrderStatus = "Submitted";

                if (_configuration["ConnectionStrings:ENV"].Equals("PROD")
                    || "PRODUCTION".Equals(sellOrder.Environment, StringComparison.CurrentCultureIgnoreCase))
                {
                    SubmitOrder(newOrder);
                }
                else
                {
                    SubmitOrderSim(newOrder);
                }
            }
            else
            {
                CheckOrderLegStatus(sellOrder, orderSummaryDict, refIdOrderDict);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="order"></param>
        /// <param name="orderSummaryDict"></param>
        /// <param name="refIdOrderDict"></param>
        private void ProcessBuyLeg(PairOrder order
            , IDictionary<string, OrderSummary> orderSummaryDict
            , IDictionary<string, string> refIdOrderDict)
        {
            PairOrderDetail buyOrder = order.BuyOrder;
            PairOrderDetailExt buyOrderExt = order.BuyOrder.OrderDetail;
            PairOrderDetailExt sellOrderExt = order.SellOrder.OrderDetail;
            string orderStatus = buyOrderExt.OrderStatus;

            if (string.IsNullOrEmpty(orderStatus))
                CheckOrderLegStatus(buyOrder, orderSummaryDict, refIdOrderDict);

            if (string.IsNullOrEmpty(orderStatus)
                && string.IsNullOrEmpty(buyOrderExt.OrderActiveFlag)
                && !buyOrderExt.TradedQty.HasValue)
            {
                CalculateOrderPrice(buyOrder, buyOrderExt, sellOrderExt);
                NewOrder newOrder = CreateNewOrder(buyOrder, buyOrderExt);
                buyOrderExt.ALMOrderStatus = "Submitted";
                buyOrderExt.OrderStatus = "Submitted";

                if (_configuration["ConnectionStrings:ENV"].Equals("PROD")
                    || "PRODUCTION".Equals(buyOrder.Environment, StringComparison.CurrentCultureIgnoreCase))
                {
                    SubmitOrder(newOrder);
                }
                else
                {
                    SubmitOrderSim(newOrder);
                }
            }
            else
            {
                CheckOrderLegStatus(buyOrder, orderSummaryDict, refIdOrderDict);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="orderDetail"></param>
        /// <param name="orderSummaryDict"></param>
        /// <param name="refIdOrderDict"></param>
        private void CheckOrderLegStatus(PairOrderDetail orderDetail
            , IDictionary<string, OrderSummary> orderSummaryDict
            , IDictionary<string, string> refIdOrderDict)
        {
            PairOrderDetailExt orderDetailExt = orderDetail.OrderDetail;
            if (!string.IsNullOrEmpty(orderDetailExt.OrderRefId))
            {
                if (refIdOrderDict.TryGetValue(orderDetailExt.OrderRefId, out string orderId))
                {
                    if (orderSummaryDict.TryGetValue(orderId, out OrderSummary orderSummary))
                    {
                        orderDetailExt.MainOrderId = orderSummary.MainOrdId;
                        orderDetailExt.OrderId = orderSummary.OrdId;
                        orderDetailExt.TradedQty = orderSummary.TrdCumQty;
                        orderDetailExt.LeavesQty = orderSummary.LeavesQty;
                        orderDetailExt.CanceledQty = orderSummary.CancQty;
                        orderDetailExt.AvgTradedPrice = orderSummary.AvgTrdPr;
                        orderDetailExt.OrderStatus = orderSummary.OrdSt;
                        orderDetailExt.OrderActiveFlag = orderSummary.OrdActFlag;
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="order"></param>
        /// <param name="orderSummaryDict"></param>
        /// <param name="refIdOrderDict"></param>
        private void CheckOrderStatus(PairOrder order
            , IDictionary<string, OrderSummary> orderSummaryDict
            , IDictionary<string, string> refIdOrderDict)
        {
            PairOrderDetailExt buyOrderExt = order.BuyOrder.OrderDetail;
            PairOrderDetailExt sellOrderExt = order.SellOrder.OrderDetail;

            //BUY order
            if (!string.IsNullOrEmpty(buyOrderExt.OrderRefId))
            {
                if (refIdOrderDict.TryGetValue(buyOrderExt.OrderRefId, out string orderId))
                {
                    if (orderSummaryDict.TryGetValue(orderId, out OrderSummary orderSummary))
                    {
                        buyOrderExt.MainOrderId = orderSummary.MainOrdId;
                        buyOrderExt.OrderId = orderSummary.OrdId;
                        buyOrderExt.TradedQty = orderSummary.TrdCumQty;
                        buyOrderExt.LeavesQty = orderSummary.LeavesQty;
                        buyOrderExt.CanceledQty = orderSummary.CancQty;
                        buyOrderExt.AvgTradedPrice = orderSummary.AvgTrdPr;
                        buyOrderExt.OrderStatus = orderSummary.OrdSt;
                        buyOrderExt.OrderActiveFlag = orderSummary.OrdActFlag;
                    }
                }
            }

            //SELL order
            if (!string.IsNullOrEmpty(sellOrderExt.OrderRefId))
            {
                if (refIdOrderDict.TryGetValue(sellOrderExt.OrderRefId, out string orderId))
                {
                    if (orderSummaryDict.TryGetValue(orderId, out OrderSummary orderSummary))
                    {
                        sellOrderExt.MainOrderId = orderSummary.MainOrdId;
                        sellOrderExt.OrderId = orderSummary.OrdId;
                        sellOrderExt.TradedQty = orderSummary.TrdCumQty;
                        sellOrderExt.LeavesQty = orderSummary.LeavesQty;
                        sellOrderExt.CanceledQty = orderSummary.CancQty;
                        sellOrderExt.AvgTradedPrice = orderSummary.AvgTrdPr;
                        sellOrderExt.OrderStatus = orderSummary.OrdSt;
                        sellOrderExt.OrderActiveFlag = orderSummary.OrdActFlag;
                    }
                }
            }

            if ("N".Equals(buyOrderExt.OrderActiveFlag) && "N".Equals(sellOrderExt.OrderActiveFlag))
                order.OrderActiveFlag = "N";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="order"></param>
        private void SubmitOrderSim(NewOrder order)
        {
            try
            {
                _simNeovestOrderPublisher.PublishMessage(order);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error submitting New/Update orders [Simulation Environment]");
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
                _neovestOrderPublisher.PublishMessage(order);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error submitting New/Update orders [Production Environment]");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IList<PairOrderDetailTO> GetPairTradeDetails()
        {
            IList<PairOrderDetailTO> list = new List<PairOrderDetailTO>();

            IDictionary<string, OrderSummary> orderSummaryDict;
            if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_SUMMARY);
            else
                orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.SIM_ORDER_SUMMARY);

            IDictionary<string, PairOrder> batchOrders = _cache.Get<IDictionary<string, PairOrder>>(CacheKeys.PAIR_TRADE_BATCH_ORDERS);
            foreach (KeyValuePair<string, PairOrder> kvp in batchOrders)
            {
                PairOrder order = kvp.Value;
                PairOrderDetailTO buyOrderTO = PopulatePairDetails(order.BuyOrder, orderSummaryDict);
                PairOrderDetailTO sellOrderTO = PopulatePairDetails(order.SellOrder, orderSummaryDict);
                list.Add(buyOrderTO);
                list.Add(sellOrderTO);
            }

            return list;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="detail"></param>
        /// <returns></returns>
        private PairOrderDetailTO PopulatePairDetails(PairOrderDetail detail
            , IDictionary<string, OrderSummary> orderSummaryDict)
        {
            PairOrderDetailExt orderDetailExt = detail.OrderDetail;
            PairOrderDetailTO result = new PairOrderDetailTO();

            //Pair Trade (inputs)
            result.MainParentId = detail.ParentId;
            result.ParentId = detail.Id;
            result.Symbol = detail.Symbol;
            result.PairRatio = detail.PairRatio;
            result.PairStrategy = detail.PairStrategy;
            result.PairRatioSetup = detail.PairRatioSetup;
            result.PairInitiateLeg = detail.PairInitiateLeg;
            result.PairSpreadOper = detail.PairSpreadOper;
            result.Locate = detail.Locate;
            result.UserName = detail.UserName;
            result.Environment = detail.Environment;
            result.OrderDest = detail.Destination;
            result.OrderBkrStrategy = detail.BrokerStrategy;
            result.OrderSide = detail.OrderSide;
            result.OrderQty = detail.OrderQty;
            result.UnhedgedQty = detail.UnhedgedQty;
            result.TickSize = detail.TickSize;
            result.PriceIter = detail.PriceIter;

            //Pair Trade Details
            result.MainOrderId = orderDetailExt.MainOrderId;
            result.OrderId = orderDetailExt.OrderId;
            result.ReferenceId = orderDetailExt.OrderRefId;
            result.RatioLast = orderDetailExt.RatioLast;
            result.RatioPassive = orderDetailExt.RatioPassive;
            result.RatioMarket = orderDetailExt.RatioMarket;
            result.RatioFilled = orderDetailExt.RatioFilled;
            result.Distance = orderDetailExt.Distance;
            result.Tradable = orderDetailExt.Tradable;
            result.LastPrc = orderDetailExt.LastPrc;
            result.BidPrc = orderDetailExt.BidPrc;
            result.AskPrc = orderDetailExt.AskPrc;
            result.Vol = orderDetailExt.Vol;
            result.BidSz = orderDetailExt.BidSz;
            result.AskSz = orderDetailExt.AskSz;
            result.ALMOrderStatus = orderDetailExt.ALMOrderStatus;
            result.OrderType = orderDetailExt.OrderType;
            result.OrderPriceType = orderDetailExt.OrderPriceType;
            result.OrderPrice = orderDetailExt.DerivedOrderPrice;

            //Order Summary (Neovest)
            if (!string.IsNullOrEmpty(orderDetailExt.OrderId)
                && orderSummaryDict.TryGetValue(orderDetailExt.OrderId, out OrderSummary orderSummary))
            {
                result.AccountName = orderSummary.AccName;
                result.AccountNumber = orderSummary.AccNum;
                result.OrderExchangeId = orderSummary.OrdExchId;
                result.OrderType = orderSummary.OrdTyp;
                result.OrderTime = orderSummary.OrdTm;
                result.AlgoParameters = orderSummary.AlgoParameters;
                result.OrderStatus = orderSummary.OrdSt;
                result.OrderActiveFlag = orderSummary.OrdActFlag;
                result.TradedMessage = orderSummary.TrdMsg;
                result.OrderStatusUpdateTime = orderSummary.OrdStatusUpdTm;
                result.OrderDateAsString = orderSummary.OrdDtAsString;

                result.TradedQty = orderSummary.TrdCumQty;
                result.CanceledQty = orderSummary.CancQty;
                result.LeavesQty = orderSummary.LeavesQty;
                result.AvgTradedPrice = orderSummary.AvgTrdPr;

                result.ALMSymbol = orderSummary.ALMSym;
                result.BBGSymbol = orderSummary.BBGSym;
                result.Sedol = orderSummary.Sedol;
                result.ISIN = orderSummary.ISIN;
                result.Cusip = orderSummary.Cusip;
                result.Currency = orderSummary.Curr;
            }

            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="order"></param>
        /// <param name="orderExt"></param>
        /// <param name="refOrderExt"></param>
        private void CalculateOrderPrice(PairOrderDetail order, PairOrderDetailExt orderExt, PairOrderDetailExt refOrderExt)
        {
            string derivedOrderType = orderExt.OrderPriceType;

            if (!string.IsNullOrEmpty(derivedOrderType))
            {
                IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
                IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);
                SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(order.Symbol, priceTickerMap, securityPriceDict);

                if (securityPrice != null)
                {
                    double? derivedOrderPrice = null;
                    if ("ASK".Equals(derivedOrderType))
                    {
                        if (securityPrice.AskPrc.HasValue && securityPrice.AskPrc.Value > 0)
                            derivedOrderPrice = securityPrice.AskPrc;
                        else if (securityPrice.LastPrc.HasValue && securityPrice.LastPrc.Value > 0)
                            derivedOrderPrice = securityPrice.LastPrc;
                    }
                    else if ("BID".Equals(derivedOrderType))
                    {
                        if (securityPrice.BidPrc.HasValue && securityPrice.BidPrc.Value > 0)
                            derivedOrderPrice = securityPrice.BidPrc;
                        else if (securityPrice.LastPrc.HasValue && securityPrice.LastPrc.Value > 0)
                            derivedOrderPrice = securityPrice.LastPrc;
                    }
                    else if ("BUY*RATIO".Equals(derivedOrderType) || "SELL*RATIO".Equals(derivedOrderType))
                    {
                        if (refOrderExt.AvgTradedPrice.HasValue && refOrderExt.AvgTradedPrice.Value > 0)
                            derivedOrderPrice = refOrderExt.AvgTradedPrice.GetValueOrDefault() * refOrderExt.RatioMarket.GetValueOrDefault();
                    }
                    else if ("BUY/RATIO".Equals(derivedOrderType) || "SELL/RATIO".Equals(derivedOrderType))
                    {
                        if (refOrderExt.AvgTradedPrice.HasValue && refOrderExt.AvgTradedPrice.Value > 0)
                            derivedOrderPrice = refOrderExt.AvgTradedPrice.GetValueOrDefault() / refOrderExt.RatioMarket.GetValueOrDefault();
                    }

                    orderExt.OrderType = "LIMIT";

                    //BUY
                    if (derivedOrderPrice.HasValue && derivedOrderPrice.Value > 0)
                    {
                        if ("B".Equals(order.OrderSide, StringComparison.CurrentCultureIgnoreCase)
                                || "BUY".Equals(order.OrderSide, StringComparison.CurrentCultureIgnoreCase))
                        {
                            orderExt.DerivedOrderPrice = Math.Floor(derivedOrderPrice.GetValueOrDefault() * 100.0) / 100.0;
                        }
                        //SELL
                        else if ("S".Equals(order.OrderSide, StringComparison.CurrentCultureIgnoreCase)
                                || "SELL".Equals(order.OrderSide, StringComparison.CurrentCultureIgnoreCase))
                        {
                            orderExt.DerivedOrderPrice = Math.Ceiling(derivedOrderPrice.GetValueOrDefault() * 100.0) / 100.0;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="pairOrders"></param>
        public void UpdateOrders(IList<PairOrderDetail> pairOrders)
        {
            try
            {
                if (pairOrders != null && pairOrders.Count > 0)
                {
                    IDictionary<string, PairOrder> batchOrders = _cache.Get<IDictionary<string, PairOrder>>(CacheKeys.PAIR_TRADE_BATCH_ORDERS);
                    foreach (PairOrderDetail order in pairOrders)
                    {
                        string parentId = order.ParentId;
                        if (batchOrders.TryGetValue(parentId, out PairOrder pairOrder))
                        {
                            _logger.LogInformation("Updating Pair Order (Old/New): " + parentId + "/" + order.Symbol + "/" + pairOrder.BuyOrder.PairRatio + "/" + order.PairRatio);
                            pairOrder.BuyOrder.PairRatio = order.PairRatio;
                            pairOrder.SellOrder.PairRatio = order.PairRatio;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing UPDATE orders");
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

                    ////Ref Index and Discount Target Fields
                    //updateOrder.RefIndex = orderSummary.RefIndex;
                    //updateOrder.RefIndexPriceType = orderSummary.RefIndexPriceType;
                    //updateOrder.RefIndexPriceBeta = orderSummary.RefIndexBeta;
                    //updateOrder.RefIndexBetaAdjType = orderSummary.RefIndexBetaAdjType;
                    //updateOrder.RefIndexPriceBetaShiftInd = orderSummary.RefIndexPriceBetaShiftInd;
                    //updateOrder.RefIndexPriceCap = orderSummary.RefIndexPriceCap;
                    //updateOrder.RefIndexPriceCapShiftInd = orderSummary.RefIndexPriceCapShiftInd;
                    //updateOrder.RefIndexMaxPrice = orderSummary.RefIndexMaxPrice;
                    //updateOrder.DiscountTarget = orderSummary.DiscountTarget;
                    //updateOrder.AutoUpdate = orderSummary.AutoUpdate;
                    //updateOrder.AutoUpdateThreshold = orderSummary.AutoUpdateThreshold;
                    //updateOrder.MarketPriceThreshold = orderSummary.MarketPriceThreshold;
                    //updateOrder.MarketPriceField = orderSummary.MarketPriceField;

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
                    }
                }
                else if (_configuration["ConnectionStrings:ENV"].Equals("DEV"))
                {
                    if ((orderSummary.OrdSt.Equals("Pending")
                        || orderSummary.OrdSt.Equals("Replaced")
                        || orderSummary.OrdSt.Equals("Replace Rejected")
                        || orderSummary.OrdSt.Equals("Partial"))
                    && string.IsNullOrEmpty(orderSummary.ALMOrdSts))
                    //&& mainOrderSummary.OrderIdList.Count < ORDER_UPDATE_COUNT)
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
    }
}
