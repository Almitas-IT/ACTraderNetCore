using aCommons.Trading;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System;
using System.Text;

namespace aACTrader.Services.EMSX
{
    public class EMSXOrderPublisher
    {
        private readonly ILogger<EMSXOrderPublisher> _logger;
        private IConfiguration _configuration { get; set; }

        private IModel _model { get; set; }
        private IConnection _connection { get; set; }

        public EMSXOrderPublisher(ILogger<EMSXOrderPublisher> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
            Initialize();
        }

        public void Initialize()
        {
            CreateConnection();
            CreateModel();
        }

        public void Stop()
        {
            try
            {
                if (_model != null && !_model.IsClosed)
                {
                    _model.Close();
                    _model.Dispose();
                }
                if (_connection != null && _connection.IsOpen)
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
                    _connection = connectionFactory.CreateConnection();

                    _logger.LogInformation("Created connection to RabbitMQ broker - DONE");

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
            _model.BasicQos(0, 1, false);

            const bool durable = false, queueAutoDelete = false, exclusive = false;
            _model.QueueDeclare(ACTraderConstants.EMSX_ORDER_QUEUE, durable, exclusive, queueAutoDelete, null);
        }

        public void PublishMessage(NewOrder order)
        {
            try
            {
                IBasicProperties basicProperties = _model.CreateBasicProperties();
                basicProperties.Persistent = true;
                basicProperties.ContentType = "application/json";
                String jsonified = JsonConvert.SerializeObject(order);
                _logger.LogInformation("Publishing Order => " + jsonified);
                byte[] buffer = Encoding.UTF8.GetBytes(jsonified);
                _model.BasicPublish("", ACTraderConstants.EMSX_ORDER_QUEUE, basicProperties, buffer);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error publishing message to queue", e);
            }
        }
    }
}