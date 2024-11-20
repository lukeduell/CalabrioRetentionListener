using CalabrioRetentionListener.Models;
using Confluent.Kafka;

namespace CalabrioRetentionListener.Services
{
    public interface IKafkaConsumerService
    {
        Task<IEnumerable<Message<TopicPartitionOffset>>> GetMessagesAsync(CancellationToken cancellationToken);
        Task GetMessagesFromPast24Hours(CancellationToken cancelToken);
        Task GetAllMessages(CancellationToken cancelToken);
        Task StartContinuousConsumer(CancellationToken cancelToken);
    }
}
