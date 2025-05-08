using aACTrader.Operations.Impl;
using aCommons;
using LazyCache;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System;
using System.Text;

namespace aACTrader.Services.Messaging
{
    public class BBGDataUpdateConsumer
    {
        private readonly ILogger<BBGDataUpdateConsumer> _logger;
        private readonly CachingService _cache;
        private readonly BBGOperations _bbgOperations;
        private readonly IConfiguration _configuration;

        private const string QUEUENAME = "BBGDataUpdate";

        private IModel _model { get; set; }
        private IConnection _connection { get; set; }

        public BBGDataUpdateConsumer(ILogger<BBGDataUpdateConsumer> logger, CachingService cache, IConfiguration configuration, BBGOperations bbgOperations)
        {
            _logger = logger;
            _cache = cache;
            _configuration = configuration;
            _bbgOperations = bbgOperations;
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
            String jsonified = Encoding.UTF8.GetString(e.Body.ToArray());
            BBGJobUpdate bbgJobUpdate = JsonConvert.DeserializeObject<BBGJobUpdate>(jsonified);

            if (bbgJobUpdate != null && !string.IsNullOrEmpty(bbgJobUpdate.JobName))
            {
                _logger.LogInformation("Received Job Update Message: " + bbgJobUpdate.JobName);
                if ("RatesUpdate".Equals(bbgJobUpdate.JobName))
                {
                    _logger.LogInformation("Procesing Job Update Message: " + bbgJobUpdate.JobName);
                    _bbgOperations.UpdateCache();
                }
                else if ("DataUpdateEurope".Equals(bbgJobUpdate.JobName))
                {
                    _logger.LogInformation("Procesing Job Update Message: " + bbgJobUpdate.JobName);
                    _bbgOperations.UpdateCacheEurope();
                }
            }
            _model.BasicAck(e.DeliveryTag, false);
        }
    }
}