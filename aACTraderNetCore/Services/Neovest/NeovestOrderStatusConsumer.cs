using aCommons;
using aCommons.Trading;
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
    public class NeovestOrderStatusConsumer
    {
        private readonly ILogger<NeovestOrderStatusConsumer> _logger;
        private CachingService _cache { get; set; }
        private IConfiguration _configuration { get; set; }

        private const string QUEUENAME = "OrderStatusQueue";

        private IModel _model { get; set; }
        private IConnection _connection { get; set; }
        private long _consumedMessages;

        public NeovestOrderStatusConsumer(ILogger<NeovestOrderStatusConsumer> logger, CachingService cache, IConfiguration configuration)
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

            try
            {
                String jsonified = Encoding.UTF8.GetString(e.Body.ToArray());
                OrderStatus orderStatusRecord = JsonConvert.DeserializeObject<OrderStatus>(jsonified);

                IDictionary<string, OrderStatus> orderSummaryDict = _cache.Get<IDictionary<string, OrderStatus>>(CacheKeys.ORDER_STATUS);

                //_logger.LogInformation("===>>> Received Order Update: " + orderStatusRecord.OrderId);

                OrderStatus record;
                if (orderSummaryDict.TryGetValue(orderStatusRecord.OrderId, out record))
                {
                    if (orderStatusRecord.TradedQty.HasValue)
                        record.TradedQty = orderStatusRecord.TradedQty;

                    if (orderStatusRecord.TradedCumulativeQty.HasValue)
                        record.TradedCumulativeQty = orderStatusRecord.TradedCumulativeQty;

                    if (orderStatusRecord.CanceledQty.HasValue && orderStatusRecord.CanceledQty > 0)
                        record.CanceledQty = orderStatusRecord.CanceledQty;

                    if (orderStatusRecord.LeavesQty.HasValue)
                        record.LeavesQty = orderStatusRecord.LeavesQty;

                    if (orderStatusRecord.TradedPrice.HasValue && orderStatusRecord.TradedPrice > 0)
                        record.TradedPrice = orderStatusRecord.TradedPrice;

                    if (!string.IsNullOrEmpty(orderStatusRecord.TradedMessage))
                        record.TradedMessage = orderStatusRecord.TradedMessage;

                    if (!string.IsNullOrEmpty(orderStatusRecord.TradeOrderStatus))
                        record.TradeOrderStatus = orderStatusRecord.TradeOrderStatus;

                    if (orderStatusRecord.OrderDate.HasValue)
                        record.OrderDate = orderStatusRecord.OrderDate;

                    if (!string.IsNullOrEmpty(orderStatusRecord.OrderTime))
                        record.OrderTime = orderStatusRecord.OrderTime;
                }
                else
                {
                    record = new OrderStatus
                    {
                        AccountNumber = orderStatusRecord.AccountNumber,
                        OrderId = orderStatusRecord.OrderId,
                        TradedQty = orderStatusRecord.TradedQty,
                        TradedCumulativeQty = orderStatusRecord.TradedCumulativeQty,
                        CanceledQty = orderStatusRecord.CanceledQty,
                        LeavesQty = orderStatusRecord.LeavesQty,
                        TradedPrice = orderStatusRecord.TradedPrice,
                        TradedMessage = orderStatusRecord.TradedMessage,
                        TradeOrderStatus = orderStatusRecord.TradeOrderStatus,
                        OrderDate = orderStatusRecord.OrderDate,
                        OrderTime = orderStatusRecord.OrderTime
                    };

                    orderSummaryDict.Add(record.OrderId, record);
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
                _logger.LogInformation("Consumed " + _consumedMessages + " Order Status real-time messages...");
                _consumedMessages = 0;
            }
        }
    }
}