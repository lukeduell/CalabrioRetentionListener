using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace CalabrioRetentionListener.Services
{
    public interface IConsumerBuilderWrapper
    {
        IConsumer<string, string> Build(ConsumerConfig consumerConfig);
    }
}
