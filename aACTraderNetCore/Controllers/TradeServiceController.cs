using aACTrader.DAO.Repository;
using aACTrader.Model;
using aACTrader.Operations.Impl;
using aACTrader.Operations.Impl.BatchOrders;
using aACTrader.Operations.Reports;
using aCommons;
using aCommons.DTO;
using aCommons.Trading;
using aCommons.Utils;
using LazyCache;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace aACTrader.Controllers
{
    [ApiController]
    public class TradeServiceController : ControllerBase
    {
        private readonly ILogger<TradeServiceController> _logger;
        private readonly CachingService _cache;
        private readonly CommonDao _commonDao;
        private readonly BrokerDataDao _brokerDataDao;
        private readonly TradingDao _tradingDao;
        private readonly PairTradingDao _pairTradingDao;
        private readonly PairTradingNVDao _pairTradingNVDao;
        private readonly CommonOperations _commonOperations;
        private readonly AllocationOperations _allocationOperations;
        private readonly PairOrderOperations _pairOrderOperations;
        private readonly NVPairOrderOperations _nvPairOrderOperations;
        private readonly BatchOrderOperations _batchOrderOperations;
        private readonly BatchOrderQueueOperations _batchOrderQueueOperations;
        private readonly TradeOrderOperations _tradeOrderOperations;
        private readonly IConfiguration _configuration;
        private readonly TradeSummaryReport _tradeSummaryReport;
        private readonly TradeSummaryReportNew _tradeSummaryReportNew;

        public TradeServiceController(ILogger<TradeServiceController> logger
            , CachingService cache
            , CommonDao commonDao
            , BrokerDataDao brokerDataDao
            , TradingDao tradingDao
            , PairTradingDao pairTradingDao
            , PairTradingNVDao pairTradingNVDao
            , CommonOperations commonOperations
            , AllocationOperations allocationOperations
            , PairOrderOperations pairOrderOperations
            , NVPairOrderOperations nvPairOrderOperations
            , BatchOrderOperations batchOrderOperations
            , TradeOrderOperations tradeOrderOperations
            , BatchOrderQueueOperations batchOrderQueueOperations
            , IConfiguration configuration
            , TradeSummaryReport tradeSummaryReport
            , TradeSummaryReportNew tradeSummaryReportNew
            )
        {
            _logger = logger;
            _cache = cache;
            _commonDao = commonDao;
            _brokerDataDao = brokerDataDao;
            _tradingDao = tradingDao;
            _pairTradingDao = pairTradingDao;
            _pairTradingNVDao = pairTradingNVDao;
            _commonOperations = commonOperations;
            _allocationOperations = allocationOperations;
            _pairOrderOperations = pairOrderOperations;
            _nvPairOrderOperations = nvPairOrderOperations;
            _batchOrderOperations = batchOrderOperations;
            _tradeOrderOperations = tradeOrderOperations;
            _batchOrderQueueOperations = batchOrderQueueOperations;
            _configuration = configuration;
            _tradeSummaryReport = tradeSummaryReport;
            _tradeSummaryReportNew = tradeSummaryReportNew;
            //_logger.LogInformation("Initializing TradeServiceController...");
        }

        [Route("/TradeService/GetLatestOrderDetails")]
        [HttpGet]
        public IList<OrderSummary> GetLatestOrderDetails()
        {
            IDictionary<string, OrderSummary> dict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_SUMMARY);
            IList<OrderSummary> list = new List<OrderSummary>(dict.Values);
            return list;
        }

        [Route("/TradeService/GetOrderDetails")]
        [HttpPost]
        public IList<OrderSummary> GetOrderDetails(OrderParameters parameters)
        {
            IDictionary<string, OrderSummary> dict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_SUMMARY);

            bool includeOrder = true;
            IList<OrderSummary> list = new List<OrderSummary>();
            foreach (OrderSummary orderSummary in dict.Values)
            {
                includeOrder = true;

                //Show All Orders Filter
                if (parameters.ShowAllOrders.Equals("N", StringComparison.CurrentCultureIgnoreCase))
                    if (!string.IsNullOrEmpty(orderSummary.BestBidOfferFlag) && orderSummary.BestBidOfferFlag.Equals("N"))
                        includeOrder = false;

                //Trader Filter
                if (!parameters.Trader.Equals("All", StringComparison.CurrentCultureIgnoreCase))
                    if (!parameters.Trader.Equals(orderSummary.Trader, StringComparison.CurrentCultureIgnoreCase))
                        includeOrder = false;

                //Show Active Orders Filter
                if (parameters.ShowActiveOrders.Equals("Y", StringComparison.CurrentCultureIgnoreCase))
                    if (!string.IsNullOrEmpty(orderSummary.OrdActFlag))
                        includeOrder = false;

                //Symbol Filter
                if (!string.IsNullOrEmpty(parameters.Symbol))
                    if (orderSummary.BBGSym.IndexOf(parameters.Symbol, StringComparison.OrdinalIgnoreCase) < 0)
                        includeOrder = false;

                if (includeOrder)
                    list.Add(orderSummary);
            }
            return list.OrderBy(o => o.OrdTm).ToList<OrderSummary>();
        }

        [Route("/TradeService/GetTradeOrderDetails")]
        [HttpPost]
        public IList<OrderSummaryTO> GetTradeOrderDetails(OrderParameters parameters)
        {
            IDictionary<string, OrderSummary> orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_SUMMARY);
            IDictionary<string, MainOrderSummary> mainOrderSummaryDict = _cache.Get<IDictionary<string, MainOrderSummary>>(CacheKeys.MAIN_ORDER_SUMMARY);
            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);

            bool includeOrder = true;
            IList<OrderSummaryTO> list = new List<OrderSummaryTO>();
            foreach (MainOrderSummary mainOrderSummary in mainOrderSummaryDict.Values)
            {
                if (orderSummaryDict.TryGetValue(mainOrderSummary.OrderId, out OrderSummary orderSummary))
                {
                    includeOrder = true;

                    //Trader Filter
                    if (!parameters.Trader.Equals("All"))
                        if (!parameters.Trader.Equals(orderSummary.Trader, StringComparison.CurrentCultureIgnoreCase))
                            includeOrder = false;

                    //ALMTrader Filter
                    if (!parameters.ALMTrader.Equals("All"))
                        if (!parameters.ALMTrader.Equals(orderSummary.ALMTrader, StringComparison.CurrentCultureIgnoreCase))
                            includeOrder = false;

                    //Show Active Orders Filter
                    if (parameters.OrderStatus.Equals("Active"))
                    {
                        if (!string.IsNullOrEmpty(orderSummary.OrdActFlag))
                            includeOrder = false;
                    }
                    else if (parameters.OrderStatus.Equals("Completed"))
                    {
                        if (!mainOrderSummary.MainOrderAvgTradedPrice.HasValue)
                            includeOrder = false;
                    }

                    //Symbol Filter
                    if (!string.IsNullOrEmpty(parameters.Symbol))
                        if (orderSummary.BBGSym.IndexOf(parameters.Symbol, StringComparison.OrdinalIgnoreCase) < 0)
                            includeOrder = false;

                    if (includeOrder)
                    {
                        OrderSummaryTO orderTO = new OrderSummaryTO()
                        {
                            MainOrderId = orderSummary.MainOrdId,
                            OrderId = orderSummary.OrdId,
                            BBGSymbol = orderSummary.BBGSym,
                            ALMSymbol = orderSummary.ALMSym,
                            OrderDate = orderSummary.OrdDt,
                            OrderTime = orderSummary.OrdTm,
                            OrderType = orderSummary.OrdTyp,
                            OrderSide = orderSummary.OrdSide,
                            OrderQty = orderSummary.OrdQty,
                            OrderOrigPrice = (orderSummary.OrdOrigPr.HasValue) ? orderSummary.OrdOrigPr : orderSummary.OrdPr,
                            OrderPrice = orderSummary.OrdPr,
                            OrderStatus = orderSummary.OrdSt,
                            OrderMemo = orderSummary.OrdMemo,
                            OrderDest = orderSummary.OrdDest,
                            OrderBkrStrategy = orderSummary.OrdBkrStrat,
                            Trader = orderSummary.Trader,
                            ALMTrader = orderSummary.ALMTrader,
                            TradedQty = orderSummary.TrdQty,
                            TradedCumulativeQty = orderSummary.TrdCumQty,
                            CanceledQty = orderSummary.CancQty,
                            LeavesQty = orderSummary.LeavesQty,
                            TradedPrice = orderSummary.TrdPr,
                            TradedMessage = orderSummary.TrdMsg,
                            OrderStatusUpdateTime = orderSummary.OrdStatusUpdTm,
                            MainOrderAvgTradedPrice = mainOrderSummary.MainOrderAvgTradedPrice,
                            CalcTradedCumulativeQty = orderSummary.OrdQty.GetValueOrDefault() - orderSummary.CancQty.GetValueOrDefault(),
                            UpdateOrderCount = mainOrderSummary.OrderIdList.Count,

                            //Spread to Market Price
                            MarketPriceSpread = orderSummary.MktPrSprd,
                            MarketPriceThreshold = orderSummary.MktPrThld,

                            //Ref Index Parameters
                            RefIndex = orderSummary.RI,
                            RefIndexPriceType = orderSummary.RIPrTyp,
                            RefIndexBeta = orderSummary.RIBeta,
                            RefIndexBetaAdjType = orderSummary.RIBetaAdjTyp,
                            RefIndexPriceCap = orderSummary.RIPrCap,
                            RefIndexMaxPrice = orderSummary.RIMaxPr,
                            RefIndexLastPrice = orderSummary.RILastPr,
                            RefIndexLivePrice = orderSummary.RILivePr,
                            RefIndexPriceChng = orderSummary.RIPrChng,
                            RefIndexPriceChngFinal = orderSummary.RIPrChngFinal,
                            RefIndexTargetPrice = orderSummary.RITgtPr,

                            //Discount Target Parameters
                            DiscountTarget = orderSummary.DscntTgt,
                            EstimatedNav = orderSummary.EstNav,
                            DiscountTargetLastNav = orderSummary.DscntTgtLastNav,
                            DiscountToLastPrice = orderSummary.DscntToLastPr,
                            DiscountToLivePrice = orderSummary.DscntToLivePr,
                        };

                        orderTO.PctCompleted = (orderTO.CalcTradedCumulativeQty / orderTO.OrderQty);

                        SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(orderTO.ALMSymbol, priceTickerMap, securityPriceDict);
                        if (securityPrice != null)
                        {
                            orderTO.PriceChange = securityPrice.PrcRtn;
                            orderTO.LastPrice = securityPrice.LastPrc;
                            orderTO.BidPrice = securityPrice.BidPrc;
                            orderTO.AskPrice = securityPrice.AskPrc;
                        }

                        list.Add(orderTO);
                    }
                }
            }
            return list.OrderByDescending(o => o.OrderTime).ToList<OrderSummaryTO>();
        }

        [Route("/TradeService/GetTradeExecutionDetails")]
        [HttpPost]
        public IList<OrderSummaryTO> GetTradeExecutionDetails(OrderParameters parameters)
        {
            IDictionary<string, OrderSummary> orderExecutionDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_EXECUTION_DETAILS);
            IDictionary<string, MainOrderSummary> mainOrderSummaryDict = _cache.Get<IDictionary<string, MainOrderSummary>>(CacheKeys.MAIN_ORDER_SUMMARY);

            DateTime currentTime = DateTime.Now;
            bool includeOrder = true;
            IList<OrderSummaryTO> list = new List<OrderSummaryTO>();
            foreach (OrderSummary orderSummary in orderExecutionDict.Values)
            {
                includeOrder = true;

                //Trader Filter
                if (!parameters.Trader.Equals("All"))
                    if (!parameters.Trader.Equals(orderSummary.Trader, StringComparison.CurrentCultureIgnoreCase))
                        includeOrder = false;

                //ALMTrader Filter
                if (!parameters.ALMTrader.Equals("All"))
                    if (!parameters.ALMTrader.Equals(orderSummary.ALMTrader, StringComparison.CurrentCultureIgnoreCase))
                        includeOrder = false;

                //Symbol Filter
                if (!string.IsNullOrEmpty(parameters.Symbol))
                    if (orderSummary.BBGSym.IndexOf(parameters.Symbol, StringComparison.OrdinalIgnoreCase) < 0)
                        includeOrder = false;

                //Time Filter
                if (!string.IsNullOrEmpty(parameters.TimeFilter) && !parameters.TimeFilter.Equals("All"))
                {
                    DateTime orderUpdateTime = DateTime.ParseExact(orderSummary.OrdStatusUpdTm, "HH:mm:ss", CultureInfo.InvariantCulture);
                    TimeSpan ts = currentTime - orderUpdateTime;
                    //_logger.LogInformation("No. of Minutes (Difference) = " + ts.TotalMinutes);

                    double totalMins = ts.TotalMinutes;
                    switch (parameters.TimeFilter)
                    {
                        case "mins1":
                            if (totalMins > 1) includeOrder = false; break;
                        case "mins5":
                            if (totalMins > 5) includeOrder = false; break;
                        case "mins10":
                            if (totalMins > 10) includeOrder = false; break;
                        case "mins30":
                            if (totalMins > 30) includeOrder = false; break;
                        case "hrs1":
                            if (totalMins > 60) includeOrder = false; break;
                        case "hrs3":
                            if (totalMins > 180) includeOrder = false; break;
                        case "hrs6":
                            if (totalMins > 360) includeOrder = false; break;
                    }
                }

                if (includeOrder)
                {
                    OrderSummaryTO orderTO = new OrderSummaryTO()
                    {
                        MainOrderId = orderSummary.MainOrdId,
                        OrderId = orderSummary.OrdId,
                        ExecutionId = orderSummary.ExecId,
                        BBGSymbol = orderSummary.BBGSym,
                        ALMSymbol = orderSummary.ALMSym,
                        OrderDate = orderSummary.OrdDt,
                        OrderTime = orderSummary.OrdTm,
                        OrderType = orderSummary.OrdTyp,
                        OrderSide = orderSummary.OrdSide,
                        OrderQty = orderSummary.OrdQty,
                        OrderPrice = orderSummary.OrdPr,
                        OrderStatus = orderSummary.OrdSt,
                        OrderMemo = orderSummary.OrdMemo,
                        OrderDest = orderSummary.OrdDest,
                        OrderBkrStrategy = orderSummary.OrdBkrStrat,
                        Trader = orderSummary.Trader,
                        ALMTrader = orderSummary.ALMTrader,
                        TradedQty = orderSummary.TrdQty,
                        TradedCumulativeQty = orderSummary.TrdCumQty,
                        CanceledQty = orderSummary.CancQty,
                        LeavesQty = orderSummary.LeavesQty,
                        TradedPrice = orderSummary.TrdPr,
                        AvgTradedPrice = orderSummary.AvgTrdPr,
                        TradedMessage = orderSummary.TrdMsg,
                        OrderStatusUpdateTime = orderSummary.OrdStatusUpdTm,

                        //Discount Target Parameters
                        DiscountTarget = orderSummary.DscntTgt,
                        EstimatedNav = orderSummary.EstNav,
                        DiscountTargetLastNav = orderSummary.DscntTgtLastNav,
                        DiscountToLastPrice = orderSummary.DscntToLastPr,
                        DiscountToLivePrice = orderSummary.DscntToLivePr,
                        LastPrice = orderSummary.LastPr,
                        BidPrice = orderSummary.BidPr,
                        AskPrice = orderSummary.AskPr,
                    };

                    if (mainOrderSummaryDict.TryGetValue(orderSummary.MainOrdId, out MainOrderSummary mainOrderSummary))
                        orderTO.OrderStatus = mainOrderSummary.OrderStatus;

                    if (orderTO.TradedCumulativeQty.HasValue)
                        orderTO.PctCompleted = (double)(orderTO.TradedCumulativeQty) / orderTO.OrderQty;
                    list.Add(orderTO);
                }
            }
            return list.OrderByDescending(o => o.OrderStatusUpdateTime).ToList<OrderSummaryTO>();
        }

        [Route("/TradeService/GetSimOrderDetails")]
        [HttpPost]
        public IList<OrderSummary> GetSimOrderDetails(OrderParameters parameters)
        {
            IDictionary<string, OrderSummary> dict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.SIM_ORDER_SUMMARY);
            IList<OrderSummary> list = new List<OrderSummary>(dict.Values);
            return list;
        }

        [Route("/TradeService/GetBatchOrderTemplates")]
        [HttpGet]
        public IList<string> GetBatchOrderTemplates()
        {
            return _tradingDao.GetBatchOrderTemplates();
        }

        [Route("/TradeService/GetSampleBatchOrderTemplates")]
        [HttpGet]
        public IList<string> GetSampleBatchOrderTemplates()
        {
            return _tradingDao.GetSampleBatchOrderTemplates();
        }

        [Route("/TradeService/GetBatchOrderTemplateDetails")]
        [HttpPost]
        public IList<BatchOrderTemplate> GetBatchOrderTemplateDetails(OrderParameters parameters)
        {
            return _tradingDao.GetBatchOrderTemplate(parameters.TemplateName);
        }

        [Route("/TradeService/SaveBatchOrderTemplateDetails")]
        [HttpPost]
        public void SaveBatchOrderTemplateDetails(IList<BatchOrderTemplate> list)
        {
            _tradingDao.SaveBatchOrderTemplate(list);
        }

        [Route("/TradeService/GetCompletedOrders")]
        [HttpPost]
        public IList<OrderSummary> GetCompletedOrders(OrderParameters parameters)
        {
            DateTime? orderDate = DateUtils.ConvertToDate(parameters.OrderDate, "yyyy-MM-dd");
            return _tradingDao.GetCompletedOrders(orderDate.GetValueOrDefault());
        }

        [Route("/TradeService/GetAllCompletedOrders")]
        [HttpPost]
        public IList<OrderSummary> GetAllCompletedOrders(OrderParameters parameters)
        {
            DateTime? orderDate = DateUtils.ConvertToDate(parameters.OrderDate, "yyyy-MM-dd");
            return _tradingDao.GetCompletedOrders(orderDate.GetValueOrDefault());
        }

        [Route("/TradeService/GetTradePositions")]
        [HttpGet]
        public IList<TradePosition> GetTradePositions()
        {
            IDictionary<string, TradePosition> dict = _cache.Get<IDictionary<string, TradePosition>>(CacheKeys.TRADE_EXECUTIONS);
            IList<TradePosition> list = new List<TradePosition>(dict.Values);
            return list;
        }

        [Route("/TradeService/SaveTrades")]
        [HttpPost]
        public void SaveTrades(IList<OrderSummary> list)
        {
            _tradingDao.SaveTrades(list);
        }

        [Route("/TradeService/GetManualTrades")]
        [HttpPost]
        public IList<OrderSummary> GetManualTrades(OrderParameters parameters)
        {
            return _tradingDao.GetManualTrades(parameters.OrderDate);
        }

        [Route("/TradeService/GetMainOrderDetails")]
        [HttpPost]
        public IList<OrderSummary> GetMainOrderDetails(OrderParameters parameters)
        {
            IDictionary<string, OrderSummary> orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_SUMMARY);
            IDictionary<string, MainOrderSummary> mainOrderSummaryDict = _cache.Get<IDictionary<string, MainOrderSummary>>(CacheKeys.MAIN_ORDER_SUMMARY);

            bool includeOrder = true;
            IList<OrderSummary> list = new List<OrderSummary>();
            foreach (KeyValuePair<string, MainOrderSummary> kvp in mainOrderSummaryDict)
            {
                MainOrderSummary mainOrderSummary = kvp.Value;
                if (orderSummaryDict.TryGetValue(mainOrderSummary.OrderId, out OrderSummary orderSummary))
                {
                    includeOrder = true;

                    //Show All Orders Filter
                    if (parameters.ShowAllOrders.Equals("N", StringComparison.CurrentCultureIgnoreCase))
                        if (!string.IsNullOrEmpty(orderSummary.BestBidOfferFlag) && orderSummary.BestBidOfferFlag.Equals("N"))
                            includeOrder = false;

                    //Trader Filter
                    if (!parameters.Trader.Equals("All", StringComparison.CurrentCultureIgnoreCase))
                        if (!parameters.Trader.Equals(orderSummary.Trader, StringComparison.CurrentCultureIgnoreCase))
                            includeOrder = false;

                    //Show Active Orders Filter
                    if (parameters.ShowActiveOrders.Equals("Y", StringComparison.CurrentCultureIgnoreCase))
                        if (!string.IsNullOrEmpty(orderSummary.OrdActFlag))
                            includeOrder = false;

                    //Symbol Filter
                    if (!string.IsNullOrEmpty(parameters.Symbol))
                        if (orderSummary.BBGSym.IndexOf(parameters.Symbol, StringComparison.OrdinalIgnoreCase) < 0)
                            includeOrder = false;

                    //Show Pair Trades Filter
                    if (parameters.ShowPairTrades.Equals("Y", StringComparison.CurrentCultureIgnoreCase))
                        if (string.IsNullOrEmpty(orderSummary.ParOrdId))
                            includeOrder = false;

                    if (includeOrder)
                    {
                        orderSummary.MainOrdAvgTrdPr = mainOrderSummary.MainOrderAvgTradedPrice;
                        orderSummary.CalcTrdCumQty = orderSummary.OrdQty.GetValueOrDefault() - orderSummary.CancQty.GetValueOrDefault();
                        orderSummary.UpdOrdCnt = mainOrderSummary.OrderIdList.Count;

                        list.Add(orderSummary);
                    }
                }
            }
            return list.OrderBy(o => o.OrdTm).ToList<OrderSummary>();
        }

        [Route("/TradeService/GetOrderHistory")]
        [HttpPost]
        public IList<OrderSummary> GetOrderHistory(OrderParameters parameters)
        {
            IDictionary<string, OrderSummary> orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_SUMMARY);
            IList<OrderSummary> list = new List<OrderSummary>();
            foreach (KeyValuePair<string, OrderSummary> kvp in orderSummaryDict)
            {
                OrderSummary orderSummary = kvp.Value;
                if (orderSummary.MainOrdId.Equals(parameters.MainOrderId))
                    list.Add(orderSummary);
            }

            return list.OrderByDescending(o => o.OrdTm).ToList<OrderSummary>();
        }

        [Route("/TradeService/GetBatchOrderTemplatesForUser")]
        [HttpPost]
        public IList<string> GetBatchOrderTemplatesForUser(OrderParameters parameters)
        {
            return _tradingDao.GetBatchOrderTemplatesForUser(parameters.Trader, null);
        }

        [Route("/TradeService/GetPairTradeTemplatesForUser")]
        [HttpPost]
        public IList<string> GetPairTradeTemplatesForUser(OrderParameters parameters)
        {
            return _pairTradingDao.GetTemplatesForUser(parameters.Trader);
        }

        [Route("/TradeService/GetPairTradeTemplate")]
        [HttpPost]
        public IList<PairOrderTemplate> GetPairTradeTemplate(OrderParameters parameters)
        {
            return _pairTradingDao.GetTemplate(parameters.TemplateName);
        }

        [Route("/TradeService/SaveASTrades")]
        [HttpPost]
        public void SaveASTrades(IList<ASTrade> trades)
        {
            _tradingDao.SaveAllTrades(trades);
        }

        [Route("/TradeService/SaveManualTrades")]
        [HttpPost]
        public void SaveManualTrades(IList<ASTrade> trades)
        {
            _tradingDao.SaveManualTrades(trades);
            _commonOperations.PopulateManualTrades();
        }

        [Route("/TradeService/SavASTradeAllocations")]
        [HttpPost]
        public void SavASTradeAllocations(IList<ASTradeAllocation> tradeAllocations)
        {
            _tradingDao.SaveAllTradeAllocations(tradeAllocations);
        }

        [Route("/TradeService/GetASTradeHistory")]
        [HttpPost]
        public IList<ASTrade> GetASTradeHistory(InputParameters parameters)
        {
            DateTime? startDate = DateUtils.ConvertToDate(parameters.StartDate, "yyyy-MM-dd");
            DateTime? endDate = DateUtils.ConvertToDate(parameters.EndDate, "yyyy-MM-dd");
            return _tradingDao.GetASTrades(parameters.Ticker, startDate.GetValueOrDefault(), endDate.GetValueOrDefault(), parameters.Source);
        }

        [Route("/TradeService/GetASTradeAllocationHistory")]
        [HttpPost]
        public IList<ASTradeAllocation> GetASTradeAllocationHistory(InputParameters parameters)
        {
            DateTime? startDate = DateUtils.ConvertToDate(parameters.StartDate, "yyyy-MM-dd");
            DateTime? endDate = DateUtils.ConvertToDate(parameters.EndDate, "yyyy-MM-dd");
            return _tradingDao.GetASTradeAllocations(parameters.Ticker, startDate.GetValueOrDefault(), endDate.GetValueOrDefault());
        }

        [Route("/TradeService/GetDailyTrades")]
        [HttpGet]
        public IList<TradeReportTO> GetDailyTrades()
        {
            return _allocationOperations.GetDailyTrades();
        }

        [Route("/TradeService/GetDailyTradeSummary")]
        [HttpPost]
        public IList<TradeSummaryTO> GetDailyTradeSummary(OrderParameters parameters)
        {
            return _allocationOperations.GetDailyTradeSummary(parameters);
        }

        [Route("/TradeService/GetDailyTradeSummaryNew")]
        [HttpPost]
        public IList<TradeSummaryTO> GetDailyTradeSummaryNew(OrderParameters parameters)
        {
            return _tradeSummaryReport.GetDailyTradeSummary(parameters);
        }

        [Route("/TradeService/GetTradeSummary")]
        [HttpPost]
        public IList<TradeSummaryReportTO> GetTradeSummary(OrderParameters parameters)
        {
            //return _tradeSummaryReportNew.GetTradeSummary
            IList<TradeSummaryReportTO> list = _cache.Get<IList<TradeSummaryReportTO>>(CacheKeys.TRADE_SUMMARY);
            return list;
        }

        [Route("/TradeService/GetLivePositionReport")]
        [HttpGet]
        public IList<PositionReportTO> GetLivePositionReport()
        {
            IList<PositionReportTO> list = _cache.Get<IList<PositionReportTO>>(CacheKeys.LIVE_POSITIONS);
            return list;
        }

        [Route("/TradeService/SavePairTradeTemplate")]
        [HttpPost]
        public void SavePairTradeTemplate(IList<PairOrderDetail> list)
        {
            _pairTradingDao.SaveTemplate(list);
        }

        [Route("/TradeService/SubmitPairTradeOrders")]
        [HttpPost]
        public void SubmitOrders(IList<PairOrderDetail> orders)
        {
            _pairOrderOperations.ProcessNewOrders(orders);
        }

        [Route("/TradeService/SubmitPairTradeOrdersSim")]
        [HttpPost]
        public void SubmitOrdersSim(IList<PairOrderDetail> orders)
        {
            _pairOrderOperations.ProcessNewOrders(orders);
        }

        [Route("/TradeService/UpdatePairTradeOrders")]
        [HttpPost]
        public void UpdatePairTradeOrders(IList<PairOrderDetail> orders)
        {
            _pairOrderOperations.UpdateOrders(orders);
        }

        [Route("/TradeService/GetPairTradeDetails")]
        [HttpGet]
        public IList<PairOrderDetailTO> GetPairTradeDetails()
        {
            return _pairOrderOperations.GetPairTradeDetails();
        }

        [Route("/TradeService/SubmitOrders")]
        [HttpPost]
        public void SubmitOrders(IList<NewOrder> orders)
        {
            _batchOrderOperations.ProcessNewOrders(orders);
        }

        [Route("/TradeService/SubmitOrdersSim")]
        [HttpPost]
        public void SubmitOrdersSim(IList<NewOrder> orders)
        {
            _batchOrderOperations.ProcessNewOrdersSim(orders);
        }

        [Route("/TradeService/SubmitOrdersToQueueSim")]
        [HttpPost]
        public void SubmitOrdersToQueueSim(IList<NewOrder> orders)
        {
            _batchOrderQueueOperations.ProcessNewOrdersSim(orders);
        }

        [Route("/TradeService/SubmitOpenAuctionOrders")]
        [HttpPost]
        public void SubmitOpenAuctionOrders(IList<NewOrder> orders)
        {
            if (orders != null && orders.Count > 0)
            {
                NewOrder order = orders[0];
                if ("PRODUCTION".Equals(order.Environment, StringComparison.CurrentCultureIgnoreCase))
                    _batchOrderOperations.ProcessOpenOrders(orders);
                else
                    _batchOrderOperations.ProcessOpenOrdersSim(orders);
            }
        }

        [Route("/TradeService/UpdateOrders")]
        [HttpPost]
        public void UpdateOrders(IList<NewOrder> orders)
        {
            _batchOrderOperations.UpdateOrders(orders);
        }

        [Route("/TradeService/SaveTradingTargets")]
        [HttpPost]
        public void SaveTradingTargets(IList<TradeTarget> list)
        {
            _tradingDao.SaveTradingTargets(list);
        }

        [Route("/TradeService/SavePairTradingTargets")]
        [HttpPost]
        public void SavePairTradingTargets(IList<PairTradeTarget> list)
        {
            _tradingDao.SavePairTradingTargets(list);
        }

        [Route("/TradeService/GetTradeStats")]
        [HttpGet]
        public IList<TradeGroupStats> GetTradeStats()
        {
            return _tradeOrderOperations.CalculateTradeStats();
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        /// NV Pair Trades
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        [Route("/TradeService/SaveNVPairTradeTemplate")]
        [HttpPost]
        public void SaveNVPairTradeTemplate(IList<PairOrderTemplateNV> list)
        {
            _pairTradingNVDao.SaveTemplate(list);
        }

        [Route("/TradeService/GetNVPairTradeTemplatesForUser")]
        [HttpPost]
        public IList<string> GetNVPairTradeTemplatesForUser(OrderParameters parameters)
        {
            return _pairTradingNVDao.GetTemplatesForUser(parameters.Trader);
        }

        [Route("/TradeService/GetNVPairTradeTemplate")]
        [HttpPost]
        public IList<PairOrderTemplateNV> GetNVPairTradeTemplate(OrderParameters parameters)
        {
            return _pairTradingNVDao.GetTemplate(parameters.TemplateName);
        }

        [Route("/TradeService/SubmitNVPairTradeOrders")]
        [HttpPost]
        public void SubmitNVPairTradeOrders(IList<NewOrder> pairOrders)
        {
            _nvPairOrderOperations.ProcessNewOrders(pairOrders);
        }

        [Route("/TradeService/GetOrderQueue")]
        [HttpPost]
        public IList<OrderSummary> GetOrderQueue(OrderParameters parameters)
        {
            IDictionary<string, OrderSummary> orderSummaryDict;

            if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_QUEUE_SUMMARY);
            else
                orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.SIM_ORDER_QUEUE_SUMMARY);

            bool includeOrder = true;
            IList<OrderSummary> list = new List<OrderSummary>();
            foreach (OrderSummary orderSummary in orderSummaryDict.Values)
            {
                includeOrder = true;

                //Show All Orders Filter
                if (parameters.ShowAllOrders.Equals("N", StringComparison.CurrentCultureIgnoreCase))
                    if (!string.IsNullOrEmpty(orderSummary.BestBidOfferFlag) && orderSummary.BestBidOfferFlag.Equals("N"))
                        includeOrder = false;

                //Trader Filter
                if (!parameters.Trader.Equals("All", StringComparison.CurrentCultureIgnoreCase))
                    if (!parameters.Trader.Equals(orderSummary.Trader, StringComparison.CurrentCultureIgnoreCase))
                        includeOrder = false;

                //Show Active Orders Filter
                if (parameters.ShowActiveOrders.Equals("Y", StringComparison.CurrentCultureIgnoreCase))
                    if (!string.IsNullOrEmpty(orderSummary.OrdActFlag))
                        includeOrder = false;

                //Symbol Filter
                if (!string.IsNullOrEmpty(parameters.Symbol))
                    if (orderSummary.BBGSym.IndexOf(parameters.Symbol, StringComparison.OrdinalIgnoreCase) < 0)
                        includeOrder = false;

                //Show Pair Trades Filter
                if (parameters.ShowPairTrades.Equals("Y", StringComparison.CurrentCultureIgnoreCase))
                    if (string.IsNullOrEmpty(orderSummary.ParOrdId))
                        includeOrder = false;

                if (includeOrder)
                    list.Add(orderSummary);
            }
            return list.OrderBy(o => o.OrdTm).ToList<OrderSummary>();
        }

        [Route("/TradeService/GetTradeExecutionSummary")]
        [HttpPost]
        public IList<TradeExecutionSummaryTO> GetTradeExecutionSummary(InputParameters parameters)
        {
            DateTime? startDate = DateUtils.ConvertToDate(parameters.StartDate, "yyyy-MM-dd");
            DateTime? endDate = DateUtils.ConvertToDate(parameters.EndDate, "yyyy-MM-dd");
            return _tradeOrderOperations.GetTradeExecutionSummary(startDate.GetValueOrDefault(), endDate.GetValueOrDefault(), parameters.Broker);
        }

        [Route("/TradeService/GetTradeTrackerDetails")]
        [HttpPost]
        public IList<TradeTrackerTO> GetTradeTrackerDetails(InputParameters parameters)
        {
            DateTime? startDate = DateUtils.ConvertToDate(parameters.StartDate, "yyyy-MM-dd");
            DateTime? endDate = DateUtils.ConvertToDate(parameters.EndDate, "yyyy-MM-dd");
            return _tradingDao.GetTradeTrackerDetails(startDate.GetValueOrDefault(), endDate.GetValueOrDefault(), parameters.Broker);
        }

        [Route("/TradeService/GetExecutedBrokers")]
        [HttpGet]
        public IList<BrokerTO> GetExecutedBrokerList()
        {
            return _brokerDataDao.GetExecutedBrokers();
        }

        [Route("/TradeService/GetASExecutingBrokers")]
        [HttpGet]
        public IList<BrokerTO> GetASExecutingBrokers()
        {
            return _brokerDataDao.GetASExecutingBrokers();
        }

        [Route("/TradeService/GetTradeSummaryHistory")]
        [HttpPost]
        public IList<TradeSummaryTO> GetTradeSummaryHistory(InputParameters parameters)
        {
            return _tradingDao.GetTradeSummaryHistory(parameters.StartDate, parameters.EndDate, parameters.Ticker);
        }

        [Route("/TradeService/AllocateTrades")]
        [HttpPost]
        public IList<TradeSummaryTO> AllocateTrades(IList<TradeSummaryTO> list)
        {
            return _tradeSummaryReport.AllocateTrades(list);
        }

        [Route("/TradeService/GetTradeCommissions")]
        [HttpPost]
        public IList<TradeExecutionSummaryTO> GetTradeCommissions(InputParameters parameters)
        {
            DateTime? startDate = DateUtils.ConvertToDate(parameters.StartDate, "yyyy-MM-dd");
            DateTime? endDate = DateUtils.ConvertToDate(parameters.EndDate, "yyyy-MM-dd");
            return _tradeOrderOperations.GetTradeCommissions(startDate.GetValueOrDefault(), endDate.GetValueOrDefault(), parameters.Broker);
        }

        [Route("/TradeService/GetBatchOrderWarnings")]
        [HttpPost]
        public IList<OrderSummaryTO> GetBatchOrderWarnings(OrderParameters parameters)
        {
            IDictionary<string, OrderSummary> orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_SUMMARY);
            IDictionary<string, MainOrderSummary> mainOrderSummaryDict = _cache.Get<IDictionary<string, MainOrderSummary>>(CacheKeys.MAIN_ORDER_SUMMARY);
            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);

            bool includeOrder = true;
            IList<OrderSummaryTO> list = new List<OrderSummaryTO>();
            foreach (MainOrderSummary mainOrderSummary in mainOrderSummaryDict.Values)
            {
                if (orderSummaryDict.TryGetValue(mainOrderSummary.OrderId, out OrderSummary orderSummary))
                {
                    includeOrder = true;
                    if (!string.IsNullOrEmpty(orderSummary.OrdActFlag))
                        includeOrder = false;

                    if (string.IsNullOrEmpty(orderSummary.ErrorFlag))
                        includeOrder = false;

                    if (includeOrder)
                    {
                        OrderSummaryTO orderTO = new OrderSummaryTO()
                        {
                            MainOrderId = orderSummary.MainOrdId,
                            OrderId = orderSummary.OrdId,
                            BBGSymbol = orderSummary.BBGSym,
                            ALMSymbol = orderSummary.ALMSym,
                            OrderDate = orderSummary.OrdDt,
                            OrderTime = orderSummary.OrdTm,
                            OrderType = orderSummary.OrdTyp,
                            OrderSide = orderSummary.OrdSide,
                            OrderQty = orderSummary.OrdQty,
                            OrderOrigPrice = (orderSummary.OrdOrigPr.HasValue) ? orderSummary.OrdOrigPr : orderSummary.OrdPr,
                            OrderPrice = orderSummary.OrdPr,
                            OrderStatus = orderSummary.OrdSt,
                            OrderMemo = orderSummary.OrdMemo,
                            OrderDest = orderSummary.OrdDest,
                            OrderBkrStrategy = orderSummary.OrdBkrStrat,
                            Trader = orderSummary.Trader,
                            ALMTrader = orderSummary.ALMTrader,
                            TradedQty = orderSummary.TrdQty,
                            TradedCumulativeQty = orderSummary.TrdCumQty,
                            CanceledQty = orderSummary.CancQty,
                            LeavesQty = orderSummary.LeavesQty,
                            TradedPrice = orderSummary.TrdPr,
                            TradedMessage = orderSummary.TrdMsg,
                            OrderStatusUpdateTime = orderSummary.OrdStatusUpdTm,
                            MainOrderAvgTradedPrice = mainOrderSummary.MainOrderAvgTradedPrice,
                            CalcTradedCumulativeQty = orderSummary.OrdQty.GetValueOrDefault() - orderSummary.CancQty.GetValueOrDefault(),
                            UpdateOrderCount = mainOrderSummary.OrderIdList.Count,
                            ErrorFlag = orderSummary.ErrorFlag,

                            //Spread to Market Price
                            MarketPriceSpread = orderSummary.MktPrSprd,
                            MarketPriceThreshold = orderSummary.MktPrThld,

                            //Ref Index Parameters
                            RefIndex = orderSummary.RI,
                            RefIndexPriceType = orderSummary.RIPrTyp,
                            RefIndexBeta = orderSummary.RIBeta,
                            RefIndexBetaAdjType = orderSummary.RIBetaAdjTyp,
                            RefIndexPriceCap = orderSummary.RIPrCap,
                            RefIndexMaxPrice = orderSummary.RIMaxPr,
                            RefIndexLastPrice = orderSummary.RILastPr,
                            RefIndexLivePrice = orderSummary.RILivePr,
                            RefIndexPriceChng = orderSummary.RIPrChng,
                            RefIndexPriceChngFinal = orderSummary.RIPrChngFinal,
                            RefIndexTargetPrice = orderSummary.RITgtPr,

                            //Discount Target Parameters
                            DiscountTarget = orderSummary.DscntTgt,
                            EstimatedNav = orderSummary.EstNav,
                            DiscountTargetLastNav = orderSummary.DscntTgtLastNav,
                            DiscountToLastPrice = orderSummary.DscntToLastPr,
                            DiscountToLivePrice = orderSummary.DscntToLivePr,
                        };

                        orderTO.PctCompleted = (orderTO.CalcTradedCumulativeQty / orderTO.OrderQty);

                        SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(orderTO.ALMSymbol, priceTickerMap, securityPriceDict);
                        if (securityPrice != null)
                        {
                            orderTO.PriceChange = securityPrice.PrcRtn;
                            orderTO.LastPrice = securityPrice.LastPrc;
                            orderTO.BidPrice = securityPrice.BidPrc;
                            orderTO.AskPrice = securityPrice.AskPrc;
                        }

                        list.Add(orderTO);
                    }
                }
            }
            return list.OrderByDescending(o => o.OrderTime).ToList<OrderSummaryTO>();
        }

        [Route("/TradeService/GetTradeActivity")]
        [HttpGet]
        public IList<TradeActivity> GetTradeActivity()
        {
            IDictionary<string, TradeActivity> dict = _tradeOrderOperations.GetTradeActivity();
            return dict.Values.ToList<TradeActivity>().OrderBy(t => t.Sym).ToList<TradeActivity>();
        }

        [Route("/TradeService/GetNeovestStatus")]
        [HttpGet]
        public IList<ServiceStatus> GetNeovestStatus()
        {
            IList<ServiceStatus> list = _cache.Get<IList<ServiceStatus>>(CacheKeys.NEOVEST_STATUS);
            return list;
        }
    }
}