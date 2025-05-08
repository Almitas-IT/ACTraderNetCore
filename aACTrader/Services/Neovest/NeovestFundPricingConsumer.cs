using aCommons;
using aCommons.DTO;
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
    public class NeovestFundPricingConsumer
    {
        private readonly ILogger<NeovestFundPricingConsumer> _logger;
        private CachingService _cache { get; set; }
        private IConfiguration _configuration { get; set; }

        private const string QUEUENAME = "NeovestFundPriceQueue";

        private IModel _model { get; set; }
        private IConnection _connection { get; set; }
        private long _consumedMessages;

        public NeovestFundPricingConsumer(ILogger<NeovestFundPricingConsumer> logger, CachingService cache, IConfiguration configuration)
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
                NeovestSecurityPrice newPriceRecord = JsonConvert.DeserializeObject<NeovestSecurityPrice>(jsonified);
                IDictionary<string, SecurityPrice> securityPriceMap = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
                SecurityPrice priceRecord;
                if (securityPriceMap.TryGetValue(newPriceRecord.Ticker, out priceRecord))
                {
                    if (newPriceRecord.PriceLast.HasValue && newPriceRecord.PriceLast > 0)
                        priceRecord.LastPrc = newPriceRecord.PriceLast;
                    priceRecord.BidPrc = newPriceRecord.PriceBid;
                    priceRecord.AskPrc = newPriceRecord.PriceAsk;
                    priceRecord.MidPrc = newPriceRecord.PriceMid;
                    priceRecord.BidSz = newPriceRecord.BidSize;
                    priceRecord.AskSz = newPriceRecord.AskSize;
                    priceRecord.Vol = newPriceRecord.Volume;
                    priceRecord.ClsPrc = newPriceRecord.PreviousClosePrice;
                    priceRecord.NetPrcChng = newPriceRecord.NetPriceChange;
                    priceRecord.PrcRtn = newPriceRecord.NetPriceChangePct;
                    priceRecord.TrdDt = newPriceRecord.TradeDate;
                    priceRecord.TrdTm = newPriceRecord.TradeTime;

                    CalculatePriceChange(priceRecord);
                    priceRecord.Src = "NV";
                }
                else
                {
                    priceRecord = new SecurityPrice
                    {
                        Ticker = newPriceRecord.Ticker,
                        LastPrc = newPriceRecord.PriceLast,
                        BidPrc = newPriceRecord.PriceBid,
                        AskPrc = newPriceRecord.PriceAsk,
                        MidPrc = newPriceRecord.PriceMid,
                        BidSz = newPriceRecord.BidSize,
                        AskSz = newPriceRecord.AskSize,
                        Vol = newPriceRecord.Volume,
                        ClsPrc = newPriceRecord.PreviousClosePrice,
                        NetPrcChng = newPriceRecord.NetPriceChange,
                        PrcRtn = newPriceRecord.NetPriceChangePct,
                        TrdDt = newPriceRecord.TradeDate,
                        TrdTm = newPriceRecord.TradeTime,
                        Src = "NV",
                        RTFlag = 1
                    };

                    CalculatePriceChange(priceRecord);
                    securityPriceMap.Add(priceRecord.Ticker, priceRecord);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing message", ex);
                throw;
            }
            _model.BasicAck(e.DeliveryTag, false);
            _consumedMessages++;
            if (_consumedMessages >= 10000)
            {
                _logger.LogInformation("Consumed " + _consumedMessages + " Neovest real-time messages so far...");
                _consumedMessages = 0;
            }
        }

        private void CalculatePriceChange(SecurityPrice priceRecord)
        {
            try
            {
                if (priceRecord.ClsPrc.GetValueOrDefault() > 0)
                {
                    priceRecord.PrcChng = ((priceRecord.LastPrc.GetValueOrDefault() + priceRecord.DvdAmt.GetValueOrDefault()) / priceRecord.ClsPrc.GetValueOrDefault()) - 1.0;
                    priceRecord.MidPrcChng = ((priceRecord.MidPrc.GetValueOrDefault() + priceRecord.DvdAmt.GetValueOrDefault()) / priceRecord.ClsPrc.GetValueOrDefault()) - 1.0;
                }
            }
            catch (Exception)
            {
            }

            try
            {
                IDictionary<string, BatchMonitorTO> dict = _cache.Get<Dictionary<string, BatchMonitorTO>>(CacheKeys.BATCH_MONITOR);
                dict["Neovest Live Prices"].LastUpdate = DateTime.Now;
            }
            catch (Exception)
            {
            }
        }
    }
}