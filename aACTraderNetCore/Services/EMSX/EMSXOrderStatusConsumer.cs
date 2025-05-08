using aCommons;
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
    public class EMSXOrderStatusConsumer
    {
        private readonly ILogger<EMSXOrderStatusConsumer> _logger;
        private CachingService _cache { get; set; }
        private IConfiguration _configuration { get; set; }

        private IModel _model { get; set; }
        private IConnection _connection { get; set; }
        private long _consumedMessages;

        public EMSXOrderStatusConsumer(ILogger<EMSXOrderStatusConsumer> logger, CachingService cache, IConfiguration configuration)
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
                _logger.LogError(ex, "Error reading from queue: " + ACTraderConstants.EMSX_ORDER_STATUS_QUEUE, ex);
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

                // wait before trying again
                // Thread.Sleep(1000);
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

            // When AutoClose is true, the last channel to close will also cause the connection to close. 
            // If it is set to true before any channel is created, the connection will close then and there.
            // _connection.AutoClose = true;

            // Configure the Quality of service for the model. Below is how what each setting means.
            // BasicQos(0="Dont send me a new message untill I’ve finshed",  1= "Send me one message at a time", false ="Apply to this Model only")
            _model.BasicQos(0, 20, false);

            const bool durable = false, queueAutoDelete = false, exclusive = false;
            _model.QueueDeclare(ACTraderConstants.EMSX_ORDER_STATUS_QUEUE, durable, exclusive, queueAutoDelete, null);

            _logger.LogInformation("Created connection to queue: " + ACTraderConstants.EMSX_ORDER_STATUS_QUEUE);
        }

        public void ReadFromQueue()
        {
            EventingBasicConsumer eventingBasicConsumer = new EventingBasicConsumer(_model);
            eventingBasicConsumer.Received += ProcessMessage;
            _model.BasicConsume(ACTraderConstants.EMSX_ORDER_STATUS_QUEUE, false, eventingBasicConsumer);
        }

        public void ProcessMessage(object sender, BasicDeliverEventArgs e)
        {
            try
            {
                string jsonified = Encoding.UTF8.GetString(e.Body.ToArray());
                EMSXOrderStatus inputData = JsonConvert.DeserializeObject<EMSXOrderStatus>(jsonified);

                IDictionary<Int32, EMSXOrderStatus> dict = _cache.Get<IDictionary<Int32, EMSXOrderStatus>>(CacheKeys.EMSX_ORDER_STATUS);
                if (dict.TryGetValue(inputData.OrdSeq.GetValueOrDefault(), out EMSXOrderStatus record))
                {
                    _logger.LogInformation("Updating Order [OrdSeq/OrdRefId/Ticker]: " + inputData.OrdSeq + "/" + inputData.OrdRefId + "/" + inputData.Ticker);
                    record.ApiSeqNum = inputData.ApiSeqNum;
                    record.Amt = inputData.Amt;
                    record.ArrivalPrc = inputData.ArrivalPrc;
                    record.AvgPrc = inputData.AvgPrc;
                    record.DayAvgPrc = inputData.DayAvgPrc;
                    record.DayFill = inputData.DayFill;
                    //record.Exch = inputData.Exch;
                    record.FillId = inputData.FillId;
                    record.Filled = inputData.Filled;
                    record.LimitPrc = inputData.LimitPrc;
                    record.ModPendStatus = inputData.ModPendStatus;
                    record.Notes = inputData.Notes;
                    //record.OrdType = inputData.OrdType;
                    record.PctRemain = inputData.PctRemain;
                    record.Principal = inputData.Principal;
                    record.ReasonCd = inputData.ReasonCd;
                    record.ReasonDesc = inputData.ReasonDesc;
                    record.RemBal = inputData.RemBal;
                    record.RoutePrc = inputData.RoutePrc;
                    record.StartAmt = inputData.StartAmt;
                    record.Status = inputData.Status;
                    record.StopPrc = inputData.StopPrc;
                    record.Working = inputData.Working;
                    //record.Acct = inputData.Acct;
                    //record.WorkPrc = inputData.WorkPrc;
                    record.TimeStamp = inputData.TimeStamp;
                }
                else
                {
                    record = new EMSXOrderStatus
                    {
                        Env = inputData.Env,
                        OrdSeq = inputData.OrdSeq,
                        OrdRefId = inputData.OrdRefId,
                        CorrelationId = inputData.CorrelationId,
                        ApiSeqNum = inputData.ApiSeqNum,
                        Amt = inputData.Amt,
                        ArrivalPrc = inputData.ArrivalPrc,
                        AssetCls = inputData.AssetCls,
                        AvgPrc = inputData.AvgPrc,
                        BlockId = inputData.BlockId,
                        Bkr = inputData.Bkr,
                        BkrComm = inputData.BkrComm,
                        DayAvgPrc = inputData.DayAvgPrc,
                        DayFill = inputData.DayFill,
                        DirBkrFlag = inputData.DirBkrFlag,
                        Exch = inputData.Exch,
                        FillId = inputData.FillId,
                        Filled = inputData.Filled,
                        Isin = inputData.Isin,
                        LimitPrc = inputData.LimitPrc,
                        ModPendStatus = inputData.ModPendStatus,
                        Notes = inputData.Notes,
                        OrdDt = inputData.OrdDt,
                        OrdTm = inputData.OrdTm,
                        OrdType = inputData.OrdType,
                        OrdSide = inputData.OrdSide,
                        OrigTrader = inputData.OrigTrader,
                        PctRemain = inputData.PctRemain,
                        PmUUID = inputData.PmUUID,
                        PortMgr = inputData.PortMgr,
                        PortNum = inputData.PortNum,
                        Principal = inputData.Principal,
                        Product = inputData.Product,
                        ReasonCd = inputData.ReasonCd,
                        ReasonDesc = inputData.ReasonDesc,
                        RemBal = inputData.RemBal,
                        RouteId = inputData.RouteId,
                        RoutePrc = inputData.RoutePrc,
                        SecName = inputData.SecName,
                        Sedol = inputData.Sedol,
                        SettleAmt = inputData.SettleAmt,
                        SettleDt = inputData.SettleDt,
                        SI = inputData.SI,
                        Side = inputData.Side,
                        StartAmt = inputData.StartAmt,
                        Status = inputData.Status,
                        StopPrc = inputData.StopPrc,
                        StratStyle = inputData.StratStyle,
                        StratType = inputData.StratType,
                        TIF = inputData.TIF,
                        TrdUUID = inputData.TrdUUID,
                        Trader = inputData.Trader,
                        TraderNotes = inputData.TraderNotes,
                        Type = inputData.Type,
                        Working = inputData.Working,
                        YKey = inputData.YKey,
                        Acct = inputData.Acct,
                        WorkPrc = inputData.WorkPrc,
                        OrigPrc = inputData.LimitPrc,
                        TimeStamp = inputData.TimeStamp,
                    };

                    record.OrdDate = DateUtils.ConvertToDate(inputData.OrdDt.GetValueOrDefault(), "YYYYMMDD");
                    record.SettleDate = DateUtils.ConvertToDate(inputData.SettleDt.GetValueOrDefault(), "YYYYMMDD");

                    if (!string.IsNullOrEmpty(inputData.Ticker))
                        record.Ticker = inputData.Ticker.Replace(" Equity", "");

                    _logger.LogInformation("Adding NEW Order [OrdSeq/OrdRefId/Ticker]: " + inputData.OrdSeq + "/" + inputData.OrdRefId + "/" + inputData.Ticker);
                    dict.Add(record.OrdSeq.GetValueOrDefault(), record);

                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing message", ex);
                throw;
            }

            _model.BasicAck(e.DeliveryTag, false);
            _consumedMessages++;
            //_logger.LogInformation("Consumed " + _consumedMessages + " EMSX Order Status messages so far...");
            if (_consumedMessages >= 10)
            {
                _logger.LogInformation("Consumed " + _consumedMessages + " EMSX Order Status messages so far...");
                _consumedMessages = 0;
            }
        }
    }
}
