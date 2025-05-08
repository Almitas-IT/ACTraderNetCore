using aCommons;
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
    public class BBGPricingConsumer
    {
        private readonly ILogger<BBGPricingConsumer> _logger;
        private CachingService _cache { get; set; }
        private IConfiguration _configuration { get; set; }

        private const string QUEUENAME = "BBGPriceQueueNew";

        private IModel _model { get; set; }
        private IConnection _connection { get; set; }
        private long ConsumedMessages;

        public BBGPricingConsumer(ILogger<BBGPricingConsumer> logger, CachingService cache, IConfiguration configuration)
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
                _logger.LogError(ex, "Error reading from queue: " + QUEUENAME);
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
                _logger.LogError(ex, "Error closing connection");
                throw;
            }
        }

        /// <summary>
        /// Connect to rabbit mq.
        /// </summary>
        /// <returns><c>true</c> if a connection to RabbitMQ has been made, <c>false</c> otherwise</returns>
        public bool CreateConnection()
        {
            _logger.LogInformation("Creating connection to RabbitMQ broker...");

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

                    _logger.LogInformation("Created connection to RabbitMQ broker");

                    return true;
                }
                catch (System.IO.EndOfStreamException ex)
                {
                    _logger.LogError(ex, "Error connecting to RabbitMQ broker");
                    return false;
                }
                catch (BrokerUnreachableException ex)
                {
                    _logger.LogError(ex, "Error connecting to RabbitMQ broker");
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
            _model.BasicQos(0, 1, false);

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
            //IBasicProperties basicProperties = e.BasicProperties;
            //_logger.LogInformation("Message received by the event based consumer. Check the debug window for details.");
            //_logger.LogInformation(string.Concat("Message received from the exchange ", e.Exchange));
            //_logger.LogInformation(string.Concat("Content type: ", basicProperties.ContentType));
            //_logger.LogInformation(string.Concat("Consumer tag: ", e.ConsumerTag));
            //_logger.LogInformation(string.Concat("Delivery tag: ", e.DeliveryTag));
            //_logger.LogInformation(string.Concat("Message: ", Encoding.UTF8.GetString(e.Body)));

            String jsonified = Encoding.UTF8.GetString(e.Body.ToArray());
            BBGSecurityPrice newPriceRecord = JsonConvert.DeserializeObject<BBGSecurityPrice>(jsonified);

            if (newPriceRecord != null && newPriceRecord.PriceLast.HasValue)
            {
                IDictionary<string, SecurityPrice> dict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);

                if (dict.TryGetValue(newPriceRecord.Ticker, out SecurityPrice priceRecord))
                {
                    priceRecord.LastPrc = newPriceRecord.PriceLast;
                    //priceRecord.LastPrc = newPriceRecord.PriceLastAct;
                    priceRecord.LastPrcAct = newPriceRecord.PriceLastAct;
                    priceRecord.BidPrc = newPriceRecord.PriceBid;
                    priceRecord.AskPrc = newPriceRecord.PriceAsk;
                    priceRecord.PrcRtn = newPriceRecord.PriceChange1DayPct;
                    priceRecord.PrcChng = newPriceRecord.PriceChange1DayPct;
                    priceRecord.ClsPrc = newPriceRecord.PreviousDayClosePrice;
                    priceRecord.TrdDt = newPriceRecord.TradeDate;
                    priceRecord.TrdTm = newPriceRecord.TradeTime;
                    priceRecord.Src = "BBG";

                    if (newPriceRecord.PriceChange1DayPct == null)
                        CalculatePriceChange(priceRecord);
                }
                else
                {
                    priceRecord = new SecurityPrice
                    {
                        Ticker = newPriceRecord.Ticker,
                        LastPrc = newPriceRecord.PriceLast,
                        //LastPrc = newPriceRecord.PriceLastAct,
                        LastPrcAct = newPriceRecord.PriceLastAct,
                        BidPrc = newPriceRecord.PriceBid,
                        AskPrc = newPriceRecord.PriceAsk,
                        ClsPrc = newPriceRecord.PreviousDayClosePrice,
                        PrcRtn = newPriceRecord.PriceChange1DayPct,
                        PrcChng = newPriceRecord.PriceChange1DayPct,
                        TrdDt = newPriceRecord.TradeDate,
                        TrdTm = newPriceRecord.TradeTime,
                        Src = "BBG"
                    };

                    if (newPriceRecord.PriceChange1DayPct == null)
                        CalculatePriceChange(priceRecord);

                    dict.Add(priceRecord.Ticker, priceRecord);
                }
            }
            _model.BasicAck(e.DeliveryTag, false);

            ConsumedMessages++;

            if (ConsumedMessages >= 1000)
            {
                _logger.LogInformation("Consumed " + ConsumedMessages + " BBG batch messages so far...");
                ConsumedMessages = 0;
            }
        }

        private void CalculatePriceChange(SecurityPrice priceRecord)
        {
            try
            {
                if (priceRecord.LastPrc.GetValueOrDefault() > 0
                    && priceRecord.ClsPrc.GetValueOrDefault() > 0)
                {
                    priceRecord.PrcRtn = (priceRecord.LastPrc.GetValueOrDefault() - priceRecord.ClsPrc.GetValueOrDefault()) / priceRecord.ClsPrc.GetValueOrDefault();
                    priceRecord.PrcChng = priceRecord.PrcRtn;
                }
            }
            catch (Exception)
            {
            }
        }
    }
}