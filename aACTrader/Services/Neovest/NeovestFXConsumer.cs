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
    public class NeovestFXConsumer
    {
        private readonly ILogger<NeovestFXConsumer> _logger;
        private CachingService _cache { get; set; }
        private IConfiguration _configuration { get; set; }
        private IModel _model { get; set; }
        private IConnection _connection { get; set; }
        private long _consumedMessages;

        private const string QUEUENAME = "FXRateQueue";

        public NeovestFXConsumer(ILogger<NeovestFXConsumer> logger, CachingService cache, IConfiguration configuration)
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
                NeovestFXRate newPriceRecord = JsonConvert.DeserializeObject<NeovestFXRate>(jsonified);
                IDictionary<string, FXRate> fxRateMap = _cache.Get<IDictionary<string, FXRate>>(CacheKeys.FX_RATES_LIVE);
                if (fxRateMap.TryGetValue(newPriceRecord.Ticker, out FXRate input))
                {
                    if (newPriceRecord.PriceLast.HasValue && newPriceRecord.PriceLast > 0)
                        input.LastPrice = newPriceRecord.PriceLast;

                    if (newPriceRecord.TradeDate.HasValue)
                        input.TradeDate = newPriceRecord.TradeDate;

                    if (!string.IsNullOrEmpty(newPriceRecord.TradeTime))
                        input.TradeTime = newPriceRecord.TradeTime;

                    input.Source = "NV";
                    PopulateBatchMonitor();
                }
                else
                {
                    input = new FXRate
                    {
                        Ticker = newPriceRecord.Ticker,
                        LastPrice = newPriceRecord.PriceLast,
                        TradeDate = newPriceRecord.TradeDate,
                        TradeTime = newPriceRecord.TradeTime,
                        Source = "NV"
                    };

                    PopulateBatchMonitor();
                    fxRateMap.Add(input.Ticker, input);
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
                _logger.LogInformation("Consumed " + _consumedMessages + " Neovest fx messages so far...");
                _consumedMessages = 0;
            }
        }

        private void PopulateBatchMonitor()
        {
            try
            {
                IDictionary<string, BatchMonitorTO> dict = _cache.Get<Dictionary<string, BatchMonitorTO>>(CacheKeys.BATCH_MONITOR);
                dict["Neovest FX Rates"].LastUpdate = DateTime.Now;
            }
            catch (Exception)
            {
            }
        }
    }
}