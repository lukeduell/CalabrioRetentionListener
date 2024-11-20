using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using RockLib.Logging;
using CommunicationCompliance.Metrics.Repositories;
using Newtonsoft.Json;
using CalabrioRetentionListener.Models;
using System.Threading;
using ProtoBuf.Meta;

namespace CalabrioRetentionListener.Services
{
    public class KafkaConsumerService : IKafkaConsumerService
    {
        public ILogger Logger { get; }
        public DelayConfig DelayConfig { get; }
        private ConsumerConfig ConsumerConfig { get; set; }
        private IConsumer<string, string> KafkaConsumer { get; set; }
        private IConsumerBuilderWrapper ConsumerBuilderWrapper { get; set; }
        private readonly IConfiguration _configuration;


        public KafkaConsumerService(ILogger logger, IConsumerBuilderWrapper consumerBuilderWrapper, IConfiguration configuration)
        {
            _configuration = configuration;
            Logger = logger;
            ConsumerBuilderWrapper = consumerBuilderWrapper;
            var env = _configuration["AppSettings:Environment"];
            var service = "RetConsumer";
            var test = _configuration[$"Kafka:{service}:{env}:GroupId"];
            ConsumerConfig = new ConsumerConfig
            {
                GroupId = _configuration[$"Kafka:{service}:{env}:GroupId"],
                BootstrapServers = _configuration[$"Kafka:{service}:{env}:BootstrapServers"],
                AutoOffsetReset = AutoOffsetReset.Earliest,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SslEndpointIdentificationAlgorithm = SslEndpointIdentificationAlgorithm.Https,
                SaslUsername = _configuration[$"Kafka:{service}:{env}:SaslUsername"],
                SaslPassword = _configuration[$"Kafka:{service}:{env}:SaslPassword"],
                EnableSslCertificateVerification = false,
                EnableAutoCommit = false,
            };
            DelayConfig = new DelayConfig
            {
                RefreshKafkaSubscriptionDelay = TimeSpan.FromSeconds(60)
            };

            KafkaConsumer = ConsumerBuilderWrapper.Build(ConsumerConfig);

            KafkaConsumer.Subscribe(_configuration[$"Kafka:{service}:{env}:GroupId"]);

            Logger.Info($"Starting Kafka Listener: {_configuration[$"Kafka:{service}:{env}:GroupId"]}");
            Console.WriteLine($"Starting Kafka Listener: {_configuration[$"Kafka:{service}:{env}:Topic1"]}");
        }
        public async Task GetMessagesFromPast24Hours(CancellationToken cancelToken)
        {
            try
            {
                Console.WriteLine("Starting configured offset Kafka consumer...");
                Logger.Info("Starting configured offset Kafka consumer...");
                await SeekToOffsetFrom24HoursAgo(cancelToken);
                
            }
            catch (Exception ex)
            {
                Console.WriteLine("An unhandled exception was thrown in the KafkaListener.");
                Logger.Error("An unhandled exception was thrown in the KafkaListener.", ex);
            }
        }

        public async Task GetAllMessages(CancellationToken cancelToken)
        {
            try
            {
                Console.WriteLine("Starting get all Kafka consumer...");
                Logger.Info("Starting get all Kafka consumer...");
                await GetAllMessagesAsync(cancelToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine("An unhandled exception was thrown in the KafkaListener.");
                Logger.Error("An unhandled exception was thrown in the KafkaListener.", ex);
            }
        }

        public async Task StartContinuousConsumer(CancellationToken cancelToken)
        {
            try
            {
                Console.WriteLine("Starting continuous Kafka consumer...");
                Logger.Info("Starting continuous Kafka consumer...");

                while (!cancelToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = KafkaConsumer.Consume(cancelToken);

                        if (consumeResult?.Message != null)
                        {
                            var message = new Message<TopicPartitionOffset>
                            {
                                Body = consumeResult.Message.Value,
                                CommitToken = consumeResult.TopicPartitionOffset
                            };

                            //process the message here
                            Console.WriteLine($"Consumed message from partition {consumeResult.Partition} at offset {consumeResult.Offset}: {consumeResult.Message.Value}");
                            Logger.Info($"Consumed message from partition {consumeResult.Partition} at offset {consumeResult.Offset}: {consumeResult.Message.Value}");

                            //manually commit the offset after processing
                            KafkaConsumer.Commit(consumeResult);
                        }
                    }
                    catch (ConsumeException consumeEx)
                    {
                        Logger.Error($"Kafka consumption error: {consumeEx.Error.Reason}", consumeEx);
                        Console.WriteLine($"Kafka consumption error: {consumeEx.Error.Reason}");
                    }
                    catch (OperationCanceledException)
                    {
                        //gracefully handle cancellation
                        Logger.Warn("Continuous Kafka consumer operation canceled.");
                        Console.WriteLine("Continuous Kafka consumer operation canceled.");
                        break;
                    }
                    catch (Exception ex)
                    {
                        Logger.Error("Unexpected error in continuous Kafka consumer loop.", ex);
                        Console.WriteLine("Unexpected error in continuous Kafka consumer loop.");
                    }
                }

                Logger.Info("Kafka continuous consumer stopped.");
                Console.WriteLine("Kafka continuous consumer stopped.");
            }
            catch (Exception ex)
            {
                Logger.Error("Fatal error in continuous Kafka consumer initialization.", ex);
                Console.WriteLine("Fatal error in continuous Kafka consumer initialization.");
            }
        }

        private async Task SeekToOffsetFrom24HoursAgo(CancellationToken cancelToken)
        {
            try
            {
                //consume once to initialize partitions
                var consumeResult = KafkaConsumer.Consume(cancelToken);
                var partitions = KafkaConsumer.Assignment;

                //check for valid partitions
                if (partitions == null || partitions.Count == 0)
                {
                    Console.WriteLine("No partitions assigned to the consumer.");
                    Logger.Info("No partitions assigned to the consumer.");
                    return;
                }

                //create a timestamp for 24 hours ago
                var configured_timestamp = new Confluent.Kafka.Timestamp(DateTime.UtcNow.AddHours(-(Int32.Parse(_configuration["AppSettings:OffsetTime"]))));
                Console.WriteLine($"Seeking to timestamp: {configured_timestamp}");
                Logger.Info($"Seeking to timestamp: {configured_timestamp}");

                //get offsets for each partition at the timestamp of (configuration) hours ago
                var topicPartitions = new List<TopicPartitionTimestamp>();
                foreach (var partition in partitions)
                {
                    topicPartitions.Add(new TopicPartitionTimestamp(partition, configured_timestamp));
                }

                //fetch offsets for the timestamp 24 hours ago
                var offsetsForTimes = KafkaConsumer.OffsetsForTimes(topicPartitions, TimeSpan.FromSeconds(double.Parse(_configuration["AppSettings:Timeout"])));

                //seek to the offset for each partition
                foreach (var offset in offsetsForTimes)
                {
                    if (offset.Offset != Offset.Unset)
                    {
                        Console.WriteLine($"Seeking partition {offset.TopicPartition.Partition} to offset {offset.Offset}");
                        Logger.Info($"Seeking partition {offset.TopicPartition.Partition} to offset {offset.Offset}");
                        KafkaConsumer.Seek(offset);
                    }
                    else
                    {
                        Console.WriteLine($"No offset found for partition {offset.TopicPartition.Partition} at 24 hours ago.");
                        Logger.Info($"No offset found for partition {offset.TopicPartition.Partition} at 24 hours ago.");
                    }
                }

                //consume all messages from the configured time onward
                var messages = new List<Message<TopicPartitionOffset>>();
                Console.WriteLine("Starting to consume messages from 24 hours ago...");
                Logger.Info("Starting to consume messages from 24 hours ago...");

                bool hasMessages = false;
                int idleCount = 0;

                //set a timeout for cancellation
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(double.Parse(_configuration["AppSettings:Timeout"]))))
                {
                    using (var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancelToken))
                    {
                        while (!cancelToken.IsCancellationRequested)
                        {
                            //consume messages from Kafka without specifying an offset
                            var result = KafkaConsumer.Consume(linkedCts.Token);

                            if (result?.Message != null)
                            {
                                hasMessages = true;
                                idleCount = 0;

                                //log or collect the consumed message
                                var message = new Message<TopicPartitionOffset>
                                {
                                    Body = result.Message.Value,
                                    CommitToken = result.TopicPartitionOffset
                                };

                                string formattedMessage = JsonConvert.SerializeObject(
                                    JsonConvert.DeserializeObject(result.Message.Value),
                                    Formatting.Indented
                                );

                                Console.WriteLine($"Received message from partition {result.Partition} at offset {result.Offset}:\n{formattedMessage}");
                                Logger.Info($"Received message from partition {result.Partition} at offset {result.Offset}:\n{formattedMessage}");
                                messages.Add(message);
                            }
                            else
                            {
                                //increment idle count if no message was received
                                idleCount++;

                                //if no messages for a certain number of iterations, break the loop
                                if (idleCount >= 3)
                                {
                                    Console.WriteLine("No more messages received for the past 15 seconds. Ending consumption.");
                                    Logger.Info("No more messages received for the past 15 seconds. Ending consumption.");
                                    break;
                                }
                            }
                        }
                    }
                }

                //if no messages were received, log and end the program
                if (!hasMessages)
                {
                    Console.WriteLine("No messages available from the last 24 hours.");
                    Logger.Info("No messages available from the last 24 hours.");
                    return;
                }

                //log total messages received
                Console.WriteLine($"Total messages retrieved from 24 hours ago: {messages.Count}");
                Logger.Info($"Total messages retrieved from 24 hours ago: {messages.Count}");
            }
            catch (ConsumeException consumeEx)
            {
                Logger.Error($"Kafka consumption error: {consumeEx.Error.Reason}", consumeEx);
                Console.WriteLine($"Kafka consumption error: {consumeEx.Error.Reason}", consumeEx);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error while seeking to offset 24 hours ago: {ex.Message}");
                Logger.Error("Error while seeking to offset 24 hours ago", ex);
            }
        }

        public async Task<IEnumerable<Message<TopicPartitionOffset>>> GetMessagesAsync(CancellationToken cancellationToken)
        {
            try
            {
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(double.Parse(_configuration["AppSettings:Timeout"])))) //set your desired timeout duration
                {
                    using (var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancellationToken))
                    {
                        var consumeResult = KafkaConsumer.Consume(linkedCts.Token);

                        if (consumeResult?.Message != null)
                        {
                            Message<TopicPartitionOffset> message = new Message<TopicPartitionOffset>()
                            {
                                Body = consumeResult.Message.Value,
                                CommitToken = consumeResult.TopicPartitionOffset
                            };
                            Console.WriteLine($"Received message: {consumeResult.Message.Value} at offset {consumeResult.Offset}");
                            return new List<Message<TopicPartitionOffset>> { message };
                        }
                        else if (consumeResult != null)
                        {
                            Console.WriteLine($"No messages found. {JsonConvert.SerializeObject(consumeResult)}");
                            Logger.Info($"No messages found. {JsonConvert.SerializeObject(consumeResult)}");
                        }
                        else
                        {
                            Console.WriteLine("Kafka ConsumeResult is null");
                            Logger.Info($"Kafka ConsumeResult is null");
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Logger.Warn("Operation timed out while waiting for Kafka message.");
                Console.WriteLine("Operation timed out while waiting for Kafka message.");
            }
            catch (ConsumeException consumeEx)
            {
                Logger.Error($"Kafka consumption error: {consumeEx.Error.Reason}", consumeEx);
                Console.WriteLine($"Kafka consumption error: {consumeEx.Error.Reason}", consumeEx);
            }
            catch (Exception ex)
            {
                Console.WriteLine("An unhandled exception was thrown in the KafkaListener.");
                Logger.Error("An unhandled exception was thrown in the KafkaListener.", ex);
                await RefreshKafkaSubscription();
            }
            return new List<Message<TopicPartitionOffset>>();
        }

        public async Task<IEnumerable<Message<TopicPartitionOffset>>> GetAllMessagesAsync(CancellationToken cancellationToken)
        {
            var messages = new List<Message<TopicPartitionOffset>>();

            try
            {
                //set a timeout for cancellation
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(double.Parse(_configuration["AppSettings:Timeout"]))))
                {
                    using (var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancellationToken))
                    {
                        Console.WriteLine("Starting to consume all available messages from the topic...");
                        Logger.Info("Starting to consume all available messages from the topic...");

                        while (!linkedCts.IsCancellationRequested)
                        {
                            //consume messages from Kafka without specifying an offset
                            var consumeResult = KafkaConsumer.Consume(linkedCts.Token);

                            if (consumeResult?.Message != null)
                            {
                                //process and add each message to the list
                                Message<TopicPartitionOffset> message = new Message<TopicPartitionOffset>
                                {
                                    Body = consumeResult.Message.Value,
                                    CommitToken = consumeResult.TopicPartitionOffset
                                };

                                Console.WriteLine($"Received message from partition {consumeResult.Partition} at offset {consumeResult.Offset}: {consumeResult.Message.Value}");
                                Logger.Info($"Received message from partition {consumeResult.Partition} at offset {consumeResult.Offset}: {consumeResult.Message.Value}");
                                messages.Add(message);
                            }
                            else if (consumeResult != null)
                            {
                                //no new messages available
                                Console.WriteLine("No more messages found.");
                                Logger.Info("No more messages found.");
                                break;
                            }
                            else
                            {
                                //null result
                                Console.WriteLine("Kafka ConsumeResult is null.");
                                Logger.Info("Kafka ConsumeResult is null.");
                                break;
                            }
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Logger.Warn("Operation canceled while waiting for Kafka messages.");
                Console.WriteLine("Operation canceled while waiting for Kafka messages.");
            }
            catch (ConsumeException consumeEx)
            {
                Logger.Error($"Kafka consumption error: {consumeEx.Error.Reason}", consumeEx);
                Console.WriteLine($"Kafka consumption error: {consumeEx.Error.Reason}");
            }
            catch (Exception ex)
            {
                Console.WriteLine("An unhandled exception occurred in Kafka consumption.");
                Logger.Error("An unhandled exception occurred in Kafka consumption.", ex);
                await RefreshKafkaSubscription(); //optional: refresh the Kafka subscription if needed
            }

            Console.WriteLine($"Total messages retrieved: {messages.Count}");
            return messages;
        }


        public async Task RefreshKafkaSubscription()
        {
            try
            {
                var env = _configuration["AppSettings:Environment"];
                var service = "RetConsumer";
                KafkaConsumer.Close();
                KafkaConsumer = ConsumerBuilderWrapper.Build(ConsumerConfig);
                KafkaConsumer.Subscribe(_configuration[$"Kafka:{service}:{env}:GroupId"]);

                //resubscribe starting from 24 hours ago
                SeekToOffsetFrom24HoursAgo(new CancellationToken());
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed to refresh kafka client. Delaying");
                Logger.Error("Failed to refresh kafka client. Delaying", ex);
                await Task.Delay(DelayConfig.RefreshKafkaSubscriptionDelay);
            }
        }

    }
}