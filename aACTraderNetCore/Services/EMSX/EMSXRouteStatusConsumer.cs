using aACTrader.Operations.Impl;
using aCommons;
using aCommons.Cef;
using aCommons.DTO;
using aCommons.Trading;
using aCommons.Utils;
using LazyCache;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections.Generic;
using System.Text;

namespace aACTrader.Services.EMSX
{
    public class EMSXRouteStatusConsumer
    {
        private readonly ILogger<EMSXRouteStatusConsumer> _logger;
        private CachingService _cache { get; set; }
        private IConfiguration _configuration { get; set; }
        private IModel _model { get; set; }
        private IConnection _connection { get; set; }
        private long _consumedMessages;

        public EMSXRouteStatusConsumer(ILogger<EMSXRouteStatusConsumer> logger, CachingService cache, IConfiguration configuration)
        {
            _logger = logger;
            _cache = cache;
            _configuration = configuration;
            Initialize();
        }

        public void Initialize()
        {
            CreateConnection();
            CreateModel();
        }

        public void Start()
        {
            try
            {
                ReadFromQueue();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error reading from queue: " + ACTraderConstants.EMSX_ROUTE_STATUS_QUEUE, ex);
                throw;
            }
        }

        public void Stop()
        {
            try
            {
                if (_model != null)
                {
                    _model.Close();
                    _model.Dispose();
                }
                if (_connection != null)
                {
                    _connection.Close();
                    _connection.Dispose();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error closing connection", ex);
                throw;
            }
        }

        /// <summary>
        /// Connect to rabbit mq.
        /// </summary>
        /// <returns><c>true</c> if a connection to RabbitMQ has been made, <c>false</c> otherwise</returns>
        public bool CreateConnection()
        {
            _logger.LogInformation("Creating connection to RabbitMQ broker");

            int attempts = 0;

            // make 3 attempts to connect to RabbitMQ just in case an interruption occurs during the connection
            while (attempts < 3)
            {
                attempts++;
                try
                {
                    var connectionFactory = new ConnectionFactory
                    {
                        HostName = _configuration["ConnectionStrings:HOSTNAME"],
                        UserName = _configuration["ConnectionStrings:USER"],
                        Password = _configuration["ConnectionStrings:PASSWORD"],
                        RequestedHeartbeat = TimeSpan.FromSeconds(30)
                    };

                    connectionFactory.AutomaticRecoveryEnabled = true;
                    connectionFactory.NetworkRecoveryInterval = TimeSpan.FromSeconds(10);
                    _connection = connectionFactory.CreateConnection();
                    _logger.LogInformation("Create connection to RabbitMQ broker - DONE");
                    return true;
                }
                catch (System.IO.EndOfStreamException ex)
                {
                    _logger.LogError(ex, "Error connecting to RabbitMQ broker", ex);
                    return false;
                }
                catch (BrokerUnreachableException ex)
                {
                    _logger.LogError(ex, "Error connecting to RabbitMQ broker", ex);
                    return false;
                }
            }

            if (_connection != null)
                _connection.Dispose();

            return false;
        }

        /// <summary>
        /// Create a model.
        /// </summary>
        private void CreateModel()
        {
            _model = _connection.CreateModel();
            _model.BasicQos(0, 20, false);
            const bool durable = false, queueAutoDelete = false, exclusive = false;
            _model.QueueDeclare(ACTraderConstants.EMSX_ROUTE_STATUS_QUEUE, durable, exclusive, queueAutoDelete, null);
            _logger.LogInformation("Created connection to queue: " + ACTraderConstants.EMSX_ROUTE_STATUS_QUEUE);
        }

        public void ReadFromQueue()
        {
            EventingBasicConsumer eventingBasicConsumer = new EventingBasicConsumer(_model);
            eventingBasicConsumer.Received += ProcessMessage;
            _model.BasicConsume(ACTraderConstants.EMSX_ROUTE_STATUS_QUEUE, false, eventingBasicConsumer);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        public void ProcessMessage(object sender, BasicDeliverEventArgs e)
        {
            try
            {
                String jsonified = Encoding.UTF8.GetString(e.Body.ToArray());
                EMSXRouteStatus inputData = JsonConvert.DeserializeObject<EMSXRouteStatus>(jsonified);

                if (inputData != null)
                {
                    _logger.LogInformation("Updating Route [OrdSeq/RouteId/OrdRefId/FillId/ApiSeqNum/Status]: " + inputData.OrdSeq + "/" + inputData.RouteId + "/" + inputData.OrdRefId + "/" + inputData.FillId + "/" + inputData.ApiSeqNum + "/" + inputData.Status);

                    IDictionary<Int32, EMSXRouteStatus> dict = _cache.Get<IDictionary<Int32, EMSXRouteStatus>>(CacheKeys.EMSX_ROUTE_STATUS);
                    EMSXRouteStatus record;
                    if (dict.TryGetValue(inputData.OrdSeq.GetValueOrDefault(), out record))
                    {
                        record.ApiSeqNum = inputData.ApiSeqNum;
                        record.Amt = inputData.Amt;
                        record.AvgPrc = inputData.AvgPrc;
                        record.DayAvgPrc = inputData.DayAvgPrc;
                        record.DayFill = inputData.DayFill;
                        //record.ExchDest = inputData.ExchDest;
                        //record.ExecBkr = inputData.ExecBkr;
                        record.FillId = inputData.FillId;
                        record.Filled = inputData.Filled;
                        record.LastCapacity = inputData.LastCapacity;
                        record.LastFillDt = inputData.LastFillDt;
                        record.LastFillTm = inputData.LastFillTm;
                        record.LastPrc = inputData.LastPrc;
                        record.LastShares = inputData.LastShares;
                        record.LimitPrc = inputData.LimitPrc;
                        record.Notes = inputData.Notes;
                        record.OrdType = inputData.OrdType;
                        record.PctRemain = inputData.PctRemain;
                        record.Principal = inputData.Principal;
                        record.ReasonCd = inputData.ReasonCd;
                        record.ReasonDesc = inputData.ReasonDesc;
                        record.RemBal = inputData.RemBal;
                        record.RouteLastUpdTm = inputData.RouteLastUpdTm;
                        record.RoutePrc = inputData.RoutePrc;
                        record.SettleAmt = inputData.SettleAmt;
                        record.Status = inputData.Status;
                        record.StopPrc = inputData.StopPrc;
                        record.UrgencyLvl = inputData.UrgencyLvl;
                        record.TimeStamp = inputData.TimeStamp;
                        record.Working = inputData.Working;
                        record.ALMStatus = null;

                        PopulateSymbols(record);
                        PopulateOrderFills(record);
                    }
                    else
                    {
                        record = new EMSXRouteStatus
                        {
                            Env = inputData.Env,
                            OrdSeq = inputData.OrdSeq,
                            OrdRefId = inputData.OrdRefId,
                            CorrelationId = inputData.CorrelationId,
                            ApiSeqNum = inputData.ApiSeqNum,
                            Acct = inputData.Acct,
                            Amt = inputData.Amt,
                            AvgPrc = inputData.AvgPrc,
                            Bkr = inputData.Bkr,
                            BkrComm = inputData.BkrComm,
                            DayAvgPrc = inputData.DayAvgPrc,
                            DayFill = inputData.DayFill,
                            ExchDest = inputData.ExchDest,
                            ExecInstrc = inputData.ExecInstrc,
                            ExecBkr = inputData.ExecBkr,
                            FillId = inputData.FillId,
                            Filled = inputData.Filled,
                            LastCapacity = inputData.LastCapacity,
                            LastFillDt = inputData.LastFillDt,
                            LastFillTm = inputData.LastFillTm,
                            LastPrc = inputData.LastPrc,
                            LastShares = inputData.LastShares,
                            LimitPrc = inputData.LimitPrc,
                            Notes = inputData.Notes,
                            OrdType = inputData.OrdType,
                            OtcFlag = inputData.OtcFlag,
                            PctRemain = inputData.PctRemain,
                            Principal = inputData.Principal,
                            ReasonCd = inputData.ReasonCd,
                            ReasonDesc = inputData.ReasonDesc,
                            RemBal = inputData.RemBal,
                            RouteAsOfDt = inputData.RouteAsOfDt,
                            RouteCreateDt = inputData.RouteCreateDt,
                            RouteCreateTm = inputData.RouteCreateTm,
                            RouteId = inputData.RouteId,
                            RouteLastUpdTm = inputData.RouteLastUpdTm,
                            RoutePrc = inputData.RoutePrc,
                            //OrigPrc = inputData.RoutePrc,
                            SettleAmt = inputData.SettleAmt,
                            SettleDt = inputData.SettleDt,
                            Status = inputData.Status,
                            StopPrc = inputData.StopPrc,
                            StratStyle = inputData.StratStyle,
                            StratType = inputData.StratType,
                            TIF = inputData.TIF,
                            TimeStamp = inputData.TimeStamp,
                            Type = inputData.Type,
                            UrgencyLvl = inputData.UrgencyLvl,
                            Working = inputData.Working,
                        };

                        PopulateSymbols(record);

                        record.LastFillDate = DateUtils.ConvertToDate(inputData.LastFillDt.GetValueOrDefault(), "YYYYMMDD");
                        record.SettleDate = DateUtils.ConvertToDate(inputData.SettleDt.GetValueOrDefault(), "YYYYMMDD");
                        record.RouteAsOfDate = DateUtils.ConvertToDate(inputData.RouteAsOfDt.GetValueOrDefault(), "YYYYMMDD");
                        record.RouteCreateDate = DateUtils.ConvertToDate(inputData.RouteCreateDt.GetValueOrDefault(), "YYYYMMDD");

                        _logger.LogInformation("Adding NEW Route [OrdSeq/RouteId/OrdRefId/FillId]: " + inputData.OrdSeq + "/" + inputData.RouteId + "/" + inputData.OrdRefId + "/" + inputData.FillId);
                        dict.Add(record.OrdSeq.GetValueOrDefault(), record);

                        _logger.LogInformation("Adding NEW Fill [OrdSeq/OrdRefId/FillId]: " + inputData.OrdSeq + "/" + inputData.OrdRefId + "/" + inputData.FillId);
                        PopulateOrderFills(record);
                        PopulateOrderUpdateParameters(record);
                    }

                    //captures all API sequences for EMSXRoute messages
                    PopulateRouteStatusHist(record);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing message", ex);
                throw;
            }

            _model.BasicAck(e.DeliveryTag, false);
            _consumedMessages++;
            //_logger.LogInformation("Consumed " + _consumedMessages + " EMSX Route Status messages so far...");

            if (_consumedMessages >= 10)
            {
                _logger.LogInformation("Consumed " + _consumedMessages + " EMSX Route Status messages so far...");
                _consumedMessages = 0;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="routeStatus"></param>
        private void PopulateOrderFills(EMSXRouteStatus routeStatus)
        {
            if (routeStatus.Filled.GetValueOrDefault() > 0)
            {
                IDictionary<Int32, IDictionary<Int32, EMSXOrderFill>> dict = _cache.Get<IDictionary<Int32, IDictionary<Int32, EMSXOrderFill>>>(CacheKeys.EMSX_ORDER_FILLS);
                if (!dict.TryGetValue(routeStatus.OrdSeq.GetValueOrDefault(), out IDictionary<Int32, EMSXOrderFill> fillDict))
                {
                    fillDict = new Dictionary<Int32, EMSXOrderFill>();
                    EMSXOrderFill orderFill = new EMSXOrderFill()
                    {
                        Env = routeStatus.Env,
                        OrdSeq = routeStatus.OrdSeq,
                        OrdRefId = routeStatus.OrdRefId,
                        OrdDate = routeStatus.RouteCreateDate,
                        Status = routeStatus.Status,
                        DayFill = routeStatus.DayFill,
                        FillId = routeStatus.FillId,
                        Filled = routeStatus.Filled,
                        LastFillDate = routeStatus.LastFillDate,
                        LastFillTm = routeStatus.LastFillTm,
                        LastPrc = routeStatus.LastPrc,
                        LastShares = routeStatus.LastShares,
                        LastCapacity = routeStatus.LastCapacity,
                        PctRemain = routeStatus.PctRemain,
                        RemBal = routeStatus.RemBal,
                        Working = routeStatus.Working,
                        AvgPrc = routeStatus.AvgPrc,
                        DayAvgPrc = routeStatus.DayAvgPrc,
                        RoutePrc = routeStatus.RoutePrc,
                    };

                    _logger.LogInformation("Adding NEW Fill (OrdSeq/FillId/FillAmt/Status): " + routeStatus.OrdSeq + "/" + routeStatus.FillId + "/" + routeStatus.Filled + "/" + routeStatus.Status);
                    fillDict.Add(orderFill.FillId.GetValueOrDefault(), orderFill);
                    dict.Add(orderFill.OrdSeq.GetValueOrDefault(), fillDict);
                }
                else
                {
                    if (fillDict.TryGetValue(routeStatus.FillId.GetValueOrDefault(), out EMSXOrderFill orderFill))
                    {
                        orderFill.Status = routeStatus.Status;
                        orderFill.DayFill = routeStatus.DayFill;
                        orderFill.Filled = routeStatus.Filled;
                        //orderFill.LastFillDate = routeStatus.LastFillDate;
                        orderFill.LastFillTm = routeStatus.LastFillTm;
                        orderFill.LastPrc = routeStatus.LastPrc;
                        orderFill.LastShares = routeStatus.LastShares;
                        orderFill.LastCapacity = routeStatus.LastCapacity;
                        orderFill.PctRemain = routeStatus.PctRemain;
                        orderFill.RemBal = routeStatus.RemBal;
                        orderFill.Working = routeStatus.Working;
                        orderFill.AvgPrc = routeStatus.AvgPrc;
                        orderFill.DayAvgPrc = routeStatus.DayAvgPrc;
                        orderFill.RoutePrc = routeStatus.RoutePrc;

                        _logger.LogInformation("Updating Fill (OrdSeq/FillId/FillAmt/Status): " + routeStatus.OrdSeq + "/" + routeStatus.FillId + "/" + routeStatus.Filled + "/" + routeStatus.Status);
                    }
                    else
                    {
                        orderFill = new EMSXOrderFill()
                        {
                            Env = routeStatus.Env,
                            OrdSeq = routeStatus.OrdSeq,
                            OrdRefId = routeStatus.OrdRefId,
                            OrdDate = routeStatus.RouteCreateDate,
                            Status = routeStatus.Status,
                            DayFill = routeStatus.DayFill,
                            FillId = routeStatus.FillId,
                            Filled = routeStatus.Filled,
                            LastFillDate = routeStatus.LastFillDate,
                            LastFillTm = routeStatus.LastFillTm,
                            LastPrc = routeStatus.LastPrc,
                            LastShares = routeStatus.LastShares,
                            LastCapacity = routeStatus.LastCapacity,
                            PctRemain = routeStatus.PctRemain,
                            RemBal = routeStatus.RemBal,
                            Working = routeStatus.Working,
                            AvgPrc = routeStatus.AvgPrc,
                            DayAvgPrc = routeStatus.DayAvgPrc,
                            RoutePrc = routeStatus.RoutePrc,
                        };
                        _logger.LogInformation("Adding NEW Fill (OrdSeq/FillId/FillAmt/Status): " + routeStatus.OrdSeq + "/" + routeStatus.FillId + "/" + routeStatus.Filled + "/" + routeStatus.Status);
                        fillDict.Add(orderFill.FillId.GetValueOrDefault(), orderFill);
                    }
                }
            }

            try
            {
                IDictionary<string, BatchMonitorTO> dict = _cache.Get<Dictionary<string, BatchMonitorTO>>(CacheKeys.BATCH_MONITOR);
                dict["EMSX Orders"].LastUpdate = DateTime.Now;
            }
            catch (Exception ex)
            {
                _logger.LogError("Error updating Batch Monitor status", ex);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="route"></param>
        private void PopulateSymbols(EMSXRouteStatus route)
        {
            try
            {
                if (string.IsNullOrEmpty(route.Ticker))
                {
                    IDictionary<Int32, EMSXOrderStatus> orderDict = _cache.Get<IDictionary<Int32, EMSXOrderStatus>>(CacheKeys.EMSX_ORDER_STATUS);
                    if (orderDict.TryGetValue(route.OrdSeq.GetValueOrDefault(), out EMSXOrderStatus order))
                    {
                        route.Ticker = order.Ticker;
                        route.OrdSide = order.OrdSide;
                        route.Trader = order.Trader;
                        route.Sedol = order.Sedol;
                        route.Isin = order.Isin;
                    }

                    if (string.IsNullOrEmpty(route.OrdRefId))
                        route.OrdRefId = route.OrdSeq.ToString();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Error populating Symbols on Route for OrdSeq: " + route.OrdSeq, ex);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="routeStatus"></param>
        private void PopulateOrderUpdateParameters(EMSXRouteStatus routeStatus)
        {
            try
            {
                IDictionary<string, NewOrder> batchOrdersByRefIdDict = _cache.Get<IDictionary<string, NewOrder>>(CacheKeys.EMSX_BATCH_ORDERS_REFID_MAP);
                if (!string.IsNullOrEmpty(routeStatus.OrdRefId)
                    && batchOrdersByRefIdDict.TryGetValue(routeStatus.OrdRefId, out NewOrder nOrder))
                {
                    //Populate ALM Trader Name
                    routeStatus.ALMTrader = nOrder.UserName;
                    routeStatus.AlgoParams = nOrder.AlgoParameters;
                    routeStatus.AlgoParamsList = nOrder.AlgoParams;

                    //Auto Update
                    routeStatus.AutoUpdate = nOrder.AutoUpdate;
                    routeStatus.AutoUpdateThld = nOrder.AutoUpdateThreshold;
                    routeStatus.MktPrThld = nOrder.MarketPriceThreshold;
                    routeStatus.MktPrFld = nOrder.MarketPriceField;

                    //Original Order Price
                    routeStatus.OrigPrc = nOrder.OrderPrice;
                    routeStatus.OrdOrigPr = nOrder.OrderPrice;

                    //Populate Ref Index Details (if available)
                    if (!string.IsNullOrEmpty(nOrder.RefIndex))
                    {
                        string refIndex = nOrder.RefIndex;
                        IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);
                        IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
                        SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(refIndex, priceTickerMap, securityPriceDict);
                        if (securityPrice != null)
                        {
                            //Ref Index
                            routeStatus.RI = refIndex;
                            routeStatus.RIPrTyp = nOrder.RefIndexPriceType;

                            //Populate Ref Index Price
                            switch (nOrder.RefIndexPriceType)
                            {
                                case "LAST":
                                    routeStatus.RILastPr = securityPrice.LastPrc;
                                    break;
                                case "MID":
                                    routeStatus.RILastPr = securityPrice.MidPrc;
                                    break;
                                case "BID":
                                    routeStatus.RILastPr = securityPrice.BidPrc;
                                    break;
                                case "ASK":
                                    routeStatus.RILastPr = securityPrice.AskPrc;
                                    break;
                            }

                            //Use User Provided Reference Price (for pre-market orders) if provided
                            //if (nOrder.RefIndexPrice.HasValue &&
                            //    routeStatus.OrdRefId.Equals(routeStatus.OrdRefId))
                            //    routeStatus.RILastPr = nOrder.RefIndexPrice;

                            //Beta
                            routeStatus.RIBeta = nOrder.RefIndexPriceBeta;
                            routeStatus.RIBetaAdjTyp = nOrder.RefIndexBetaAdjType;
                            routeStatus.RIPrBetaShiftInd = nOrder.RefIndexPriceBetaShiftInd;
                            //Price Cap
                            routeStatus.RIPrCap = nOrder.RefIndexPriceCap;
                            routeStatus.RIPrCapShiftInd = nOrder.RefIndexPriceCapShiftInd;
                            routeStatus.RIMaxPr = nOrder.RefIndexMaxPrice;
                        }
                    }
                    //Populate Discount Target (if available)
                    else if (nOrder.DiscountTarget.HasValue)
                    {
                        //Discount
                        routeStatus.DscntTgt = nOrder.DiscountTarget;
                        routeStatus.DscntTgtAdj = nOrder.DiscountTargetAdj;
                        routeStatus.EstNavTyp = nOrder.EstNavType;
                        //Price Cap
                        routeStatus.RIPrCap = nOrder.RefIndexPriceCap;
                        routeStatus.RIPrCapShiftInd = nOrder.RefIndexPriceCapShiftInd;
                        routeStatus.RIMaxPr = nOrder.RefIndexMaxPrice;
                    }
                }

                //string symbol = !string.IsNullOrEmpty(routeStatus.ALMSym) ? routeStatus.ALMSym : routeStatus.BBGSym;
                IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
                FundForecast fundForecast = FundForecastOperations.GetFundForecast(routeStatus.Ticker, fundForecastDict);
                if (fundForecast != null)
                {
                    routeStatus.DscntTgtLastNav = fundForecast.EstNav;
                    routeStatus.DscntToLivePr = fundForecast.PDLastPrc;
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error populating Ref Index/Discount Target details");
            }
        }

        private void PopulateRouteStatusHist(EMSXRouteStatus routeStatus)
        {
            IDictionary<string, EMSXRouteStatus> dict = _cache.Get<IDictionary<string, EMSXRouteStatus>>(CacheKeys.EMSX_ROUTE_STATUS_HIST);
            string id = routeStatus.OrdSeq.GetValueOrDefault() + "|" + routeStatus.ApiSeqNum.GetValueOrDefault();
            if (!dict.ContainsKey(id))
            {
                EMSXRouteStatus record = new EMSXRouteStatus
                {
                    Env = routeStatus.Env,
                    OrdSeq = routeStatus.OrdSeq,
                    OrdRefId = routeStatus.OrdRefId,
                    CorrelationId = routeStatus.CorrelationId,
                    ApiSeqNum = routeStatus.ApiSeqNum,
                    Acct = routeStatus.Acct,
                    Amt = routeStatus.Amt,
                    AvgPrc = routeStatus.AvgPrc,
                    Bkr = routeStatus.Bkr,
                    BkrComm = routeStatus.BkrComm,
                    DayAvgPrc = routeStatus.DayAvgPrc,
                    DayFill = routeStatus.DayFill,
                    ExchDest = routeStatus.ExchDest,
                    ExecInstrc = routeStatus.ExecInstrc,
                    ExecBkr = routeStatus.ExecBkr,
                    FillId = routeStatus.FillId,
                    Filled = routeStatus.Filled,
                    LastCapacity = routeStatus.LastCapacity,
                    LastFillDt = routeStatus.LastFillDt,
                    LastFillTm = routeStatus.LastFillTm,
                    LastPrc = routeStatus.LastPrc,
                    LastShares = routeStatus.LastShares,
                    LimitPrc = routeStatus.LimitPrc,
                    Notes = routeStatus.Notes,
                    OrdType = routeStatus.OrdType,
                    OtcFlag = routeStatus.OtcFlag,
                    PctRemain = routeStatus.PctRemain,
                    Principal = routeStatus.Principal,
                    ReasonCd = routeStatus.ReasonCd,
                    ReasonDesc = routeStatus.ReasonDesc,
                    RemBal = routeStatus.RemBal,
                    RouteAsOfDt = routeStatus.RouteAsOfDt,
                    RouteAsOfDate = routeStatus.RouteAsOfDate,
                    RouteCreateDt = routeStatus.RouteCreateDt,
                    RouteCreateDate = routeStatus.RouteCreateDate,
                    RouteCreateTm = routeStatus.RouteCreateTm,
                    RouteId = routeStatus.RouteId,
                    RouteLastUpdTm = routeStatus.RouteLastUpdTm,
                    RoutePrc = routeStatus.RoutePrc,
                    SettleAmt = routeStatus.SettleAmt,
                    SettleDt = routeStatus.SettleDt,
                    Status = routeStatus.Status,
                    StopPrc = routeStatus.StopPrc,
                    StratStyle = routeStatus.StratStyle,
                    StratType = routeStatus.StratType,
                    TIF = routeStatus.TIF,
                    TimeStamp = routeStatus.TimeStamp,
                    Type = routeStatus.Type,
                    UrgencyLvl = routeStatus.UrgencyLvl,
                    Working = routeStatus.Working,
                    Ticker = routeStatus.Ticker,
                    OrdSide = routeStatus.OrdSide,
                    Trader = routeStatus.Trader,
                    Sedol = routeStatus.Sedol,
                    Isin = routeStatus.Isin,
                };
                dict.Add(id, record);
            }

            //update route order active flag
            if (!string.IsNullOrEmpty(routeStatus.Status))
            {
                if (routeStatus.Status.Equals("CANCEL")
                    || routeStatus.Status.Equals("REJECTED")
                    || routeStatus.Status.Equals("COMPLETED")
                    || routeStatus.Status.Equals("EXPIRED")
                    || routeStatus.Status.Equals("FILLED")
                    || routeStatus.Status.Equals("ASSIGN")
                    )
                {
                    routeStatus.OrdActFlag = "N";
                }
            }
        }
    }
}
