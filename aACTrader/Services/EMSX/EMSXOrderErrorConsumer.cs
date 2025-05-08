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
    public class EMSXOrderErrorConsumer
    {
        private readonly ILogger<EMSXOrderErrorConsumer> _logger;
        private CachingService _cache { get; set; }
        private IConfiguration _configuration { get; set; }

        private IModel _model { get; set; }
        private IConnection _connection { get; set; }
        private long _consumedMessages;

        public EMSXOrderErrorConsumer(ILogger<EMSXOrderErrorConsumer> logger, CachingService cache, IConfiguration configuration)
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
                _logger.LogError(ex, "Error reading from queue: " + ACTraderConstants.EMSX_ORDER_ERROR_QUEUE, ex);
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
            // When AutoClose is true, the last channel to close will also cause the connection to close. 
            // If it is set to true before any channel is created, the connection will close then and there.
            // _connection.AutoClose = true;
            // Configure the Quality of service for the model. Below is how what each setting means.
            // BasicQos(0="Dont send me a new message untill I’ve finshed",  1= "Send me one message at a time", false ="Apply to this Model only")
            _model.BasicQos(0, 20, false);
            const bool durable = false, queueAutoDelete = false, exclusive = false;
            _model.QueueDeclare(ACTraderConstants.EMSX_ORDER_ERROR_QUEUE, durable, exclusive, queueAutoDelete, null);
            _logger.LogInformation("Created connection to queue: " + ACTraderConstants.EMSX_ORDER_ERROR_QUEUE);
        }

        public void ReadFromQueue()
        {
            EventingBasicConsumer eventingBasicConsumer = new EventingBasicConsumer(_model);
            eventingBasicConsumer.Received += ProcessMessage;
            _model.BasicConsume(ACTraderConstants.EMSX_ORDER_ERROR_QUEUE, false, eventingBasicConsumer);
        }

        public void ProcessMessage(object sender, BasicDeliverEventArgs e)
        {
            try
            {
                string jsonified = Encoding.UTF8.GetString(e.Body.ToArray());
                EMSXOrderError inputData = JsonConvert.DeserializeObject<EMSXOrderError>(jsonified);
                IList<EMSXOrderError> list = _cache.Get<IList<EMSXOrderError>>(CacheKeys.EMSX_ORDER_ERRORS);
                EMSXOrderError record = new EMSXOrderError
                {
                    Env = inputData.Env,
                    CorrelationId = inputData.CorrelationId,
                    ErrorCode = inputData.ErrorCode,
                    ErrorMessage = inputData.ErrorMessage,
                    Notes = inputData.Notes,
                    ErrorType = inputData.ErrorType,
                    OrdDt = inputData.OrdDt,
                    AsOfTime = DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss")
                };
                record.OrdDate = DateUtils.ConvertToDate(inputData.OrdDt.GetValueOrDefault(), "YYYYMMDD");
                //_logger.LogInformation("Adding Order Error: " + record.ErrorMessage);
                list.Add(record);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing message", ex);
                throw;
            }
            _model.BasicAck(e.DeliveryTag, false);
            _consumedMessages++;
            if (_consumedMessages >= 5)
            {
                _logger.LogInformation("Consumed " + _consumedMessages + " EMSX Order Error messages so far...");
                _consumedMessages = 0;
            }
        }
    }
}