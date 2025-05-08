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

namespace aACTrader.Services.Messaging
{
    public class NeovestOrderSummaryConsumer
    {
        private readonly ILogger<NeovestOrderSummaryConsumer> _logger;
        private CachingService _cache { get; set; }
        private IConfiguration _configuration { get; set; }
        private IModel _model { get; set; }
        private IConnection _connection { get; set; }
        private long _consumedMessages;

        private const string QUEUENAME = "OrderSummaryQueue";

        public NeovestOrderSummaryConsumer(ILogger<NeovestOrderSummaryConsumer> logger, CachingService cache, IConfiguration configuration)
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
                _logger.LogError(ex, "Error reading from queue: " + QUEUENAME, ex);
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

            // make 3 attempts to connect to RabbitMQ just in case an interruption occurs during the connection
            int attempts = 0;
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
            _model.QueueDeclare(QUEUENAME, durable, exclusive, queueAutoDelete, null);
            _logger.LogInformation("Created connection to queue: " + QUEUENAME);
        }

        public void ReadFromQueue()
        {
            EventingBasicConsumer eventingBasicConsumer = new EventingBasicConsumer(_model);
            eventingBasicConsumer.Received += ProcessMessage;
            _model.BasicConsume(QUEUENAME, false, eventingBasicConsumer);
        }

        public void ProcessMessage(object sender, BasicDeliverEventArgs e)
        {
            try
            {
                String jsonified = Encoding.UTF8.GetString(e.Body.ToArray());
                //_logger.LogInformation("...JSON: " + jsonified);
                OrderSummary orderSummaryRecord = JsonConvert.DeserializeObject<OrderSummary>(jsonified);
                IDictionary<string, OrderSummary> orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_SUMMARY);
                //_logger.LogInformation("===>>> Received Order: " + orderSummaryRecord.OrderId);

                OrderSummary record;
                if (orderSummaryDict.TryGetValue(orderSummaryRecord.OrdId, out record))
                {
                    if (!string.IsNullOrEmpty(orderSummaryRecord.OrdSt))
                    {
                        ///////////////////////////////////////////////////////////////////////////////////////////////////
                        //Order Original Fields
                        ///////////////////////////////////////////////////////////////////////////////////////////////////
                        if (orderSummaryRecord.OrdQty.HasValue)
                            record.OrdQty = orderSummaryRecord.OrdQty;

                        if (orderSummaryRecord.OrdPr.HasValue)
                            record.OrdPr = orderSummaryRecord.OrdPr;

                        if (orderSummaryRecord.OrdStopPr.HasValue)
                            record.OrdStopPr = orderSummaryRecord.OrdStopPr;

                        if (!string.IsNullOrEmpty(orderSummaryRecord.OrdTyp))
                            record.OrdTyp = orderSummaryRecord.OrdTyp;

                        if (!string.IsNullOrEmpty(orderSummaryRecord.OrdDest))
                            record.OrdDest = orderSummaryRecord.OrdDest;

                        if (!string.IsNullOrEmpty(orderSummaryRecord.OrdBkrStrat))
                            record.OrdBkrStrat = orderSummaryRecord.OrdBkrStrat;

                        if (orderSummaryRecord.OrdDt.HasValue)
                            record.OrdDt = orderSummaryRecord.OrdDt;

                        ///////////////////////////////////////////////////////////////////////////////////////////////////
                        //Order Trade Fields
                        ///////////////////////////////////////////////////////////////////////////////////////////////////
                        record.OrdSt = orderSummaryRecord.OrdSt;

                        //For Change Order requests, sometimes the price update will reflect on "Replace" order event and not "Replace Ack" order event
                        if (orderSummaryRecord.OrdSt.Equals("Replaced"))
                        {
                            _logger.LogInformation("[Replace Order Price]: "
                                + orderSummaryRecord.Sym
                                + "/" + orderSummaryRecord.MainOrdId
                                + "/" + orderSummaryRecord.OrdId
                                + "/" + record.OrdPr
                                + "/" + orderSummaryRecord.OrdPr);
                            record.OrdPr = orderSummaryRecord.OrdPr;
                        }
                    }

                    if (!string.IsNullOrEmpty(orderSummaryRecord.CancOrdId))
                        record.CancOrdId = orderSummaryRecord.CancOrdId;

                    if (orderSummaryRecord.TrdQty.HasValue)
                        record.TrdQty = orderSummaryRecord.TrdQty;

                    if (orderSummaryRecord.TrdPr.HasValue)
                        record.TrdPr = orderSummaryRecord.TrdPr;

                    if (orderSummaryRecord.AvgTrdPr.HasValue)
                        record.AvgTrdPr = orderSummaryRecord.AvgTrdPr;

                    if (!string.IsNullOrEmpty(orderSummaryRecord.TrdMsg))
                        record.TrdMsg = orderSummaryRecord.TrdMsg;

                    if (!string.IsNullOrEmpty(orderSummaryRecord.OrdStatusUpdTm))
                        record.OrdStatusUpdTm = orderSummaryRecord.OrdStatusUpdTm;

                    record.NVExecId = orderSummaryRecord.NVExecId;
                    record.IsAnExec = orderSummaryRecord.IsAnExec;

                    record.LeavesQty = orderSummaryRecord.LeavesQty;
                    record.CancQty = orderSummaryRecord.CancQty;
                    if (orderSummaryRecord.IsAnExec.Equals("Y"))
                        record.TrdCumQty = CommonUtils.AddNullableInts(record.TrdCumQty, orderSummaryRecord.TrdQty);

                    //Populate Main Order Status
                    UpdateOrderStatus(record);
                }
                else
                {
                    _logger.LogInformation("Adding NEW Order -> MainOrderId/OrderId: " + orderSummaryRecord.MainOrdId + "/" + orderSummaryRecord.OrdId);

                    record = new OrderSummary
                    {
                        Key = orderSummaryRecord.Sym + "|" + orderSummaryRecord.OrdSide,
                        AccNum = orderSummaryRecord.AccNum,
                        AccName = orderSummaryRecord.AccName,
                        ParOrdId = orderSummaryRecord.ParOrdId,
                        MainOrdId = orderSummaryRecord.MainOrdId,
                        OrdId = orderSummaryRecord.OrdId,
                        CancOrdId = orderSummaryRecord.CancOrdId,
                        TrkId = orderSummaryRecord.TrkId,
                        RefId = orderSummaryRecord.RefId,
                        NVExecIdLng = orderSummaryRecord.NVExecIdLng,
                        Sym = orderSummaryRecord.Sym,
                        OrdDt = orderSummaryRecord.OrdDt,
                        OrdTm = orderSummaryRecord.OrdTm,
                        OrdTyp = orderSummaryRecord.OrdTyp,
                        OrdSide = orderSummaryRecord.OrdSide,
                        OrdQty = orderSummaryRecord.OrdQty,
                        OrdExchId = orderSummaryRecord.OrdExchId,
                        OrdExch = orderSummaryRecord.OrdExch,
                        OrdPr = orderSummaryRecord.OrdPr,
                        OrdStopPr = orderSummaryRecord.OrdStopPr,
                        OrdSt = orderSummaryRecord.OrdSt,
                        OrdMemo = orderSummaryRecord.OrdMemo,
                        OrdDest = orderSummaryRecord.OrdDest,
                        OrdBkrStrat = orderSummaryRecord.OrdBkrStrat,
                        Trader = orderSummaryRecord.Trader,
                        AlgoParameters = orderSummaryRecord.AlgoParameters,
                        ISIN = orderSummaryRecord.ISIN,
                        Sedol = orderSummaryRecord.Sedol,
                        Cusip = orderSummaryRecord.Cusip,
                        Curr = orderSummaryRecord.Curr,
                        TrdSrc = "NV",
                        BidAskPrFlag = 0,
                        OptEqPos = orderSummaryRecord.OptEqPos,
                        OptPos = orderSummaryRecord.OptPos,
                        OptFill = orderSummaryRecord.OptFill,

                        //Status Columns
                        TrdQty = orderSummaryRecord.TrdQty,
                        CancQty = orderSummaryRecord.CancQty,
                        LeavesQty = orderSummaryRecord.LeavesQty,
                        TrdPr = orderSummaryRecord.TrdPr,
                        AvgTrdPr = orderSummaryRecord.AvgTrdPr,
                        TrdMsg = orderSummaryRecord.TrdMsg,
                        OrdStatusUpdTm = orderSummaryRecord.OrdStatusUpdTm,

                        //Is An Order Execution
                        IsAnExec = orderSummaryRecord.IsAnExec
                    };

                    //Symbol
                    record.BBGSym = TranslateSymbol(orderSummaryRecord.Sym, orderSummaryRecord.BBGSym, record);

                    //Pair Trade Spread
                    if (!string.IsNullOrEmpty(record.ParOrdId))
                        record.PairTrdSprd = GetPairTradeSpread(orderSummaryRecord);

                    //Order Date
                    record.OrdDtAsString = DateUtils.ConvertDate(record.OrdDt, "yyyy-MM-dd");

                    //Add Order to Order->Symbol Map
                    //To identify best bid/offer trades
                    AddToOrderMap(record);

                    //Populate ALM Trader Name
                    GetTraderName(orderSummaryRecord, record);

                    //Populate Main Order Status
                    UpdateOrderStatus(record);

                    orderSummaryDict.Add(record.OrdId, record);

                    //_logger.LogInformation("Adding NEW Order -> MainOrderId/OrderId: " + orderSummaryRecord.MainOrderId + "/" + orderSummaryRecord.OrderId + " - DONE");
                }

                //Order Execution
                if (record != null && record.IsAnExec.Equals("Y"))
                {
                    IDictionary<string, OrderSummary> orderExecutionDetails = _cache.Get<Dictionary<string, OrderSummary>>(CacheKeys.ORDER_EXECUTION_DETAILS);
                    if (!orderExecutionDetails.TryGetValue(orderSummaryRecord.ExecId, out OrderSummary executionRecord))
                    {
                        executionRecord = new OrderSummary
                        {
                            AccNum = record.AccNum,
                            AccName = record.AccName,
                            ParOrdId = record.ParOrdId,
                            MainOrdId = record.MainOrdId,
                            OrdId = record.OrdId,
                            CancOrdId = record.CancOrdId,
                            TrkId = record.TrkId,
                            RefId = record.RefId,
                            NVExecIdLng = record.NVExecIdLng,
                            Sym = record.Sym,
                            OrdDt = record.OrdDt,
                            OrdTm = record.OrdTm,
                            OrdTyp = record.OrdTyp,
                            OrdSide = record.OrdSide,
                            OrdQty = record.OrdQty,
                            OrdExchId = record.OrdExchId,
                            OrdExch = record.OrdExch,
                            OrdPr = record.OrdPr,
                            OrdStopPr = record.OrdStopPr,
                            OrdSt = record.OrdSt,
                            OrdMemo = record.OrdMemo,
                            OrdDest = record.OrdDest,
                            OrdBkrStrat = record.OrdBkrStrat,
                            Trader = record.Trader,
                            ALMTrader = record.ALMTrader,
                            AlgoParameters = record.AlgoParameters,
                            ISIN = record.ISIN,
                            Sedol = record.Sedol,
                            Cusip = record.Cusip,
                            Curr = record.Curr,
                            BBGSym = record.BBGSym,
                            ALMSym = record.ALMSym,
                            ExecId = orderSummaryRecord.ExecId,
                            NVExecId = orderSummaryRecord.NVExecId,

                            //Status Columns
                            TrdQty = record.TrdQty,
                            TrdCumQty = record.TrdCumQty,
                            CancQty = record.CancQty,
                            LeavesQty = record.LeavesQty,
                            TrdPr = record.TrdPr,
                            AvgTrdPr = record.AvgTrdPr,
                            TrdMsg = record.TrdMsg,
                            OrdStatusUpdTm = record.OrdStatusUpdTm,

                            //Discount
                            DscntTgt = record.DscntTgt,
                            //DscntTgtLastNav = record.DscntTgtLastNav,
                            //DscntToLastPr = record.DscntToLastPr,
                            //DscntToLivePr = record.DscntToLivePr,
                        };

                        orderExecutionDetails.Add(orderSummaryRecord.ExecId, executionRecord);

                        AddExecutionId(executionRecord);
                    }
                    else
                    {
                        executionRecord.NVExecId = orderSummaryRecord.NVExecId;

                        ///////////////////////////////////////////////////////////////////////////////////////////////////
                        //Order Original Fields
                        ///////////////////////////////////////////////////////////////////////////////////////////////////
                        if (record.OrdQty.HasValue)
                            executionRecord.OrdQty = record.OrdQty;

                        if (record.OrdPr.HasValue)
                            executionRecord.OrdPr = record.OrdPr;

                        if (record.OrdStopPr.HasValue)
                            executionRecord.OrdStopPr = record.OrdStopPr;

                        if (!string.IsNullOrEmpty(record.OrdTyp))
                            executionRecord.OrdTyp = record.OrdTyp;

                        if (!string.IsNullOrEmpty(record.OrdDest))
                            executionRecord.OrdDest = record.OrdDest;

                        if (!string.IsNullOrEmpty(record.OrdBkrStrat))
                            executionRecord.OrdBkrStrat = record.OrdBkrStrat;

                        if (record.OrdDt.HasValue)
                            executionRecord.OrdDt = record.OrdDt;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing message", ex);
                throw;
            }

            _model.BasicAck(e.DeliveryTag, false);
            _consumedMessages++;

            if (_consumedMessages >= 5000)
            {
                _logger.LogInformation("Consumed " + _consumedMessages + " Order Summary real-time messages...");
                _consumedMessages = 0;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="symbol"></param>
        /// <param name="bbgSymbol"></param>
        /// <param name="orderSummary"></param>
        /// <returns></returns>
        private string TranslateSymbol(string symbol, string bbgSymbol, OrderSummary orderSummary)
        {
            try
            {
                if (string.IsNullOrEmpty(bbgSymbol))
                    return symbol;

                IDictionary<string, string> sourceTickerToAlmTickerDict = _cache.Get<IDictionary<string, string>>(CacheKeys.SOURCE_PRICE_TICKER_MAP);
                if (sourceTickerToAlmTickerDict.TryGetValue(bbgSymbol, out string almSymbol))
                    orderSummary.ALMSym = almSymbol;

                return bbgSymbol.Replace(" Equity", "");
            }
            catch (Exception)
            {
            }
            return bbgSymbol;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="order"></param>
        private void AddToOrderMap(OrderSummary order)
        {
            try
            {
                IDictionary<string, IDictionary<string, OrderSummary>> orderSummaryBySymbolDict = _cache.Get<IDictionary<string, IDictionary<string, OrderSummary>>>(CacheKeys.ORDER_SUMMARY_BY_SYMBOL);

                string key = string.Empty;
                if (!string.IsNullOrEmpty(order.ParOrdId))
                    key = order.OrdId + "|" + "PAIR_TRADE";
                else
                    key = order.BBGSym + "|" + order.OrdSide;

                IDictionary<string, OrderSummary> dict;
                if (orderSummaryBySymbolDict.TryGetValue(key, out dict))
                {
                    if (!dict.ContainsKey(order.OrdId))
                        dict.Add(order.OrdId, order);
                }
                else
                {
                    dict = new Dictionary<string, OrderSummary>
                    {
                        { order.OrdId, order }
                    };
                    orderSummaryBySymbolDict.Add(key, dict);
                }

                //Capture Ref Id to Main Order Id Map
                IDictionary<string, string> refIdOrderDict = _cache.Get<IDictionary<string, string>>(CacheKeys.REFID_ORDERS_MAP);
                if (!string.IsNullOrEmpty(order.RefId))
                {
                    if (!refIdOrderDict.TryGetValue(order.RefId, out string orderId))
                        refIdOrderDict.Add(order.RefId, order.OrdId);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error adding Order to Order Map: " + order.OrdId, ex);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="order"></param>
        /// <returns></returns>
        private double? GetPairTradeSpread(OrderSummary order)
        {
            double? spread = null;
            string orderId = order.OrdId;

            try
            {
                if (!string.IsNullOrEmpty(order.AlgoParameters))
                {
                    string[] aParams = order.AlgoParameters.Split(',');
                    foreach (string param in aParams)
                    {
                        if (!string.IsNullOrEmpty(param) &&
                            param.Trim().StartsWith("Spread Limit:", StringComparison.CurrentCultureIgnoreCase))
                        {
                            string[] spreadParams = param.Trim().Split(' ');
                            spread = DataConversionUtils.ConvertToDouble(spreadParams[2]);
                            break;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error getting Pair Trade spread for OrderId: " + orderId);
            }
            return spread;
        }

        /// <summary>
        /// Gets ALM Trader Name when Order is submitted from Batch Orders screen
        /// </summary>
        /// <param name="nvOrder"></param>
        /// <returns></returns>
        private string GetTraderName(OrderSummary nvOrder, OrderSummary almOrder)
        {
            try
            {
                IDictionary<string, NewOrder> batchOrdersByRefIdDict = _cache.Get<IDictionary<string, NewOrder>>(CacheKeys.BATCH_ORDERS_REFID_MAP);
                if (!string.IsNullOrEmpty(nvOrder.RefId)
                    && batchOrdersByRefIdDict.TryGetValue(nvOrder.RefId, out NewOrder nOrder))
                {
                    //Populate ALM Trader Name
                    almOrder.ALMTrader = nOrder.UserName;

                    //Auto Update
                    almOrder.AutoUpdate = nOrder.AutoUpdate;
                    almOrder.AutoUpdateThld = nOrder.AutoUpdateThreshold;
                    almOrder.MktPrThld = nOrder.MarketPriceThreshold;
                    almOrder.MktPrFld = nOrder.MarketPriceField;
                    almOrder.IsPairTrd = nOrder.IsPairTrade;
                    almOrder.UpdateAlgoParams = nOrder.UpdateAlgoParams;

                    //Original Order Price
                    almOrder.OrdOrigPr = nOrder.OrderPrice;

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
                            almOrder.RI = refIndex;
                            almOrder.RIPrTyp = nOrder.RefIndexPriceType;

                            //Populate Ref Index Price
                            switch (nOrder.RefIndexPriceType)
                            {
                                case "LAST":
                                    almOrder.RILastPr = securityPrice.LastPrc;
                                    break;
                                case "MID":
                                    almOrder.RILastPr = securityPrice.MidPrc;
                                    break;
                                case "BID":
                                    almOrder.RILastPr = securityPrice.BidPrc;
                                    break;
                                case "ASK":
                                    almOrder.RILastPr = securityPrice.AskPrc;
                                    break;
                            }

                            //Use User Provided Reference Price (for pre-market orders) if provided
                            if (nOrder.RefIndexPrice.HasValue &&
                                almOrder.MainOrdId.Equals(almOrder.OrdId))
                                almOrder.RILastPr = nOrder.RefIndexPrice;

                            //Beta
                            almOrder.RIBeta = nOrder.RefIndexPriceBeta;
                            almOrder.RIBetaAdjTyp = nOrder.RefIndexBetaAdjType;
                            almOrder.RIPrBetaShiftInd = nOrder.RefIndexPriceBetaShiftInd;
                            //Price Cap
                            almOrder.RIPrCap = nOrder.RefIndexPriceCap;
                            almOrder.RIPrCapShiftInd = nOrder.RefIndexPriceCapShiftInd;
                            almOrder.RIMaxPr = nOrder.RefIndexMaxPrice;
                        }
                    }
                    //Populate Discount Target (if available)
                    else if (nOrder.DiscountTarget.HasValue)
                    {
                        //Discount
                        almOrder.DscntTgt = nOrder.DiscountTarget;
                        almOrder.DscntTgtAdj = nOrder.DiscountTargetAdj;
                        almOrder.EstNavTyp = nOrder.EstNavType;
                        //Price Cap
                        almOrder.RIPrCap = nOrder.RefIndexPriceCap;
                        almOrder.RIPrCapShiftInd = nOrder.RefIndexPriceCapShiftInd;
                        almOrder.RIMaxPr = nOrder.RefIndexMaxPrice;
                    }
                }

                string symbol = !string.IsNullOrEmpty(almOrder.ALMSym) ? almOrder.ALMSym : almOrder.BBGSym;
                IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
                FundForecast fundForecast = FundForecastOperations.GetFundForecast(symbol, fundForecastDict);
                if (fundForecast != null)
                {
                    almOrder.DscntTgtLastNav = fundForecast.EstNav;
                    almOrder.DscntToLivePr = fundForecast.PDLastPrc;
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error populating Ref Index/Discount Target details");
            }
            return null;
        }

        /// <summary>
        /// Update Order Status (mark in-active orders)
        /// </summary>
        /// <param name="orderSummary"></param>
        private void UpdateOrderStatus(OrderSummary orderSummary)
        {
            try
            {
                if (!string.IsNullOrEmpty(orderSummary.OrdSt))
                {
                    if (orderSummary.OrdSt.Equals("Canceled")
                        || orderSummary.OrdSt.Equals("Canceled/ Partial")
                        || orderSummary.OrdSt.Equals("Completed")
                        || orderSummary.OrdSt.Equals("Executed")
                        || orderSummary.OrdSt.Equals("Rejected")
                        || orderSummary.OrdSt.Equals("Replace Rejected")
                        )
                    {
                        orderSummary.OrdActFlag = "N";
                    }
                }

                //Capture Order Id and Status of latest Order within Main Order
                IDictionary<string, MainOrderSummary> mainOrderSummaryDict = _cache.Get<IDictionary<string, MainOrderSummary>>(CacheKeys.MAIN_ORDER_SUMMARY);
                if (mainOrderSummaryDict.TryGetValue(orderSummary.MainOrdId, out MainOrderSummary mainOrderSummary))
                {
                    mainOrderSummary.OrderId = orderSummary.OrdId;
                    mainOrderSummary.OrderStatus = orderSummary.OrdSt;

                    if (orderSummary.AvgTrdPr.HasValue)
                        mainOrderSummary.MainOrderAvgTradedPrice = orderSummary.AvgTrdPr;

                    if (!mainOrderSummary.OrderIdList.Contains(orderSummary.OrdId))
                        mainOrderSummary.OrderIdList.Add(orderSummary.OrdId);
                }
                else
                {
                    mainOrderSummary = new MainOrderSummary();
                    mainOrderSummary.MainOrderId = orderSummary.MainOrdId;
                    mainOrderSummary.OrderId = orderSummary.OrdId;
                    mainOrderSummary.OrderStatus = orderSummary.OrdSt;

                    if (orderSummary.AvgTrdPr.HasValue)
                        mainOrderSummary.MainOrderAvgTradedPrice = orderSummary.AvgTrdPr;

                    if (!mainOrderSummary.OrderIdList.Contains(orderSummary.OrdId))
                        mainOrderSummary.OrderIdList.Add(orderSummary.OrdId);

                    mainOrderSummaryDict.Add(orderSummary.MainOrdId, mainOrderSummary);
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error updating Order Status: " + orderSummary.OrdId);
            }

            try
            {
                IDictionary<string, BatchMonitorTO> dict = _cache.Get<Dictionary<string, BatchMonitorTO>>(CacheKeys.BATCH_MONITOR);
                dict["Neovest Orders"].LastUpdate = DateTime.Now;
            }
            catch (Exception)
            {
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="orderExecution"></param>
        private void AddExecutionId(OrderSummary orderExecution)
        {
            IDictionary<string, MainOrderSummary> mainOrderSummaryDict = _cache.Get<IDictionary<string, MainOrderSummary>>(CacheKeys.MAIN_ORDER_SUMMARY);
            if (mainOrderSummaryDict.TryGetValue(orderExecution.MainOrdId, out MainOrderSummary mainOrderSummary))
            {
                if (!mainOrderSummary.ExecutionIdList.Contains(orderExecution.ExecId))
                    mainOrderSummary.ExecutionIdList.Add(orderExecution.ExecId);
            }

            try
            {
                string symbol = !string.IsNullOrEmpty(orderExecution.ALMSym) ? orderExecution.ALMSym : orderExecution.BBGSym;
                IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);
                IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
                SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(symbol, priceTickerMap, securityPriceDict);
                if (securityPrice != null)
                {
                    orderExecution.LastPr = securityPrice.LastPrc;
                    orderExecution.BidPr = securityPrice.BidPrc;
                    orderExecution.AskPr = securityPrice.AskPrc;
                }

                IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
                FundForecast fundForecast = FundForecastOperations.GetFundForecast(symbol, fundForecastDict);
                if (fundForecast != null)
                {
                    orderExecution.EstNav = fundForecast.EstNav;
                    orderExecution.PubNav = fundForecast.LastDvdAdjNav;
                    orderExecution.DscntToLivePr = DataConversionUtils.CalculateReturn(orderExecution.LastPr, fundForecast.EstNav);
                    orderExecution.DscntToLastPr = DataConversionUtils.CalculateReturn(orderExecution.TrdPr, fundForecast.EstNav);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Error calculating discount to Fill/Last price for Execution Id: " + orderExecution.ExecId, ex);
            }
        }
    }
}