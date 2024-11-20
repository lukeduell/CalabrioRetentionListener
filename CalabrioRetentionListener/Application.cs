using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RockLib.Logging;
using RockLib.Logging.DependencyInjection;
using CalabrioRetentionListener.Services;
using CalabrioRetentionListener.Models;
using Microsoft.Extensions.Configuration;

namespace CalabrioRetentionListener
{
    public class Application
    {
        private readonly RockLib.Logging.ILogger _splunkLogger;
        private readonly IKafkaConsumerService _kafkaService;
        private readonly IConfiguration _configuration;
        private readonly IKafkaAckProducerService _kafkaAckProducerService;

        public Application(RockLib.Logging.ILogger splunkLog, RockLib.Logging.ILogger splunkLogger, IKafkaConsumerService kafkaConsumerService, IConfiguration configuration, IKafkaAckProducerService kafkaAckProducerService)
        {
            _splunkLogger = splunkLog;
            _kafkaService = kafkaConsumerService;
            _configuration = configuration;
            _kafkaAckProducerService = kafkaAckProducerService;
        }
        public async Task Run()
        {
            try
            {
                CancellationToken cancellationToken = new CancellationToken();
                _splunkLogger.Info("Starting all tasks");

                //This is the message that is sent to Kafka edit it as you wish
                await _kafkaAckProducerService.SendAcknowledgementMessageAsync(new AcknowledgementMessage
                {
                    Id = "1",
                    CorrelationId = "1",
                    Source = "1",
                    SpecVersion = "1",
                    Type = "1",
                    Time = "1",
                    DataContentType = "1",
                    Subject = "1",
                    Data = new AcknowledgementData
                    {
                        AcknowledgementId = "1",
                        AcknowledgementTimestamp = "1",
                        ActionTaken = "1"
                    }
                });


                //switch (_configuration["RunType"])
                //{
                //    case "Continuous":
                //        await _kafkaService.StartContinuousConsumer(cancellationToken);
                //        break;
                //    case "Past24Hours":
                //        await _kafkaService.GetMessagesFromPast24Hours(cancellationToken);
                //        break;
                //    case "All":
                //        await _kafkaService.GetAllMessages(cancellationToken);
                //        break;
                //    default:
                //        await _kafkaService.GetMessagesFromPast24Hours(cancellationToken);
                //        break;
                //}
                _splunkLogger.Info("Finished all tasks");
            }
            catch (Exception ex)
            {
                _splunkLogger.Error(ex.Message);
            }
            _splunkLogger.Info("Done doing the done did do");
        }
    }
}
