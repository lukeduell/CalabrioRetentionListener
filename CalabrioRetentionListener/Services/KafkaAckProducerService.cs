using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using RockLib.Logging;
using System;
using System.Threading.Tasks;
using CalabrioRetentionListener.Models;
using ProtoBuf.Meta;

namespace CalabrioRetentionListener.Services
{
    public class KafkaAckProducerService : IKafkaAckProducerService
    {
        private readonly IProducer<string, string> _kafkaProducer;
        private readonly ProducerConfig _producerConfig;
        private readonly ILogger _logger;
        private readonly IConfiguration _configuration;
        private const int MaxRetryAttempts = 5;
        private const int RetryBackoffMs = 5000; //5-second delay between retries

        public KafkaAckProducerService(ILogger logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
            var env = _configuration["AppSettings:Environment"];
            var service = "AckProducer";
            _producerConfig = new ProducerConfig
            {
                BootstrapServers = _configuration[$"Kafka:{service}:{env}:BootstrapServers"],
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SslEndpointIdentificationAlgorithm = SslEndpointIdentificationAlgorithm.Https,
                SaslUsername = _configuration[$"Kafka:{service}:{env}:SaslUsername"],
                SaslPassword = _configuration[$"Kafka:{service}:{env}:SaslPassword"],
                EnableSslCertificateVerification = false,
            };

            _kafkaProducer = new ProducerBuilder<string, string>(_producerConfig).Build();
        }

        public async Task SendAcknowledgementMessageAsync(AcknowledgementMessage ackMessage)
        {
            var env = _configuration["AppSettings:Environment"];
            var service = "AckProducer";
            string topic = _configuration[$"Kafka:{service}:{env}:Topic1"]; //or specify another topic if needed
            string key = ackMessage.Id; //use the message ID as the key
            string value = JsonConvert.SerializeObject(ackMessage, Formatting.None);

            var message = new Message<string, string>
            {
                Key = key,
                Value = value
            };

            int retryCount = 0;

            while (retryCount < MaxRetryAttempts)
            {
                try
                {
                    _logger.Info($"Sending message {message}");
                    var deliveryResult = await _kafkaProducer.ProduceAsync(topic, message);

                    _logger.Info($"Message delivered to {deliveryResult.TopicPartitionOffset}");
                    Console.WriteLine($"Message delivered to {deliveryResult.TopicPartitionOffset}");

                    //message was successfully delivered
                    break;
                }
                catch (ProduceException<string, string> ex)
                {
                    retryCount++;
                    _logger.Error($"Kafka produce error: {ex.Error.Reason}. Retry attempt {retryCount} of {MaxRetryAttempts}", ex);
                    Console.WriteLine($"Kafka produce error: {ex.Error.Reason}. Retry attempt {retryCount} of {MaxRetryAttempts}");

                    if (retryCount >= MaxRetryAttempts)
                    {
                        _logger.Error("Max retry attempts reached. Message could not be delivered.", ex);
                        throw; //or handle according to your application's needs
                    }

                    //wait before retrying
                    await Task.Delay(RetryBackoffMs);
                }
                catch (Exception ex)
                {
                    _logger.Error("An unexpected error occurred while producing message.", ex);
                    Console.WriteLine("An unexpected error occurred while producing message.");
                    throw; //or handle according to your application's needs
                }
            }
        }

        public void Dispose()
        {
            _kafkaProducer.Flush(TimeSpan.FromSeconds(10));
            _kafkaProducer.Dispose();
        }
    }
}
