using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static CalabrioRetentionListener.Services.ConsumerBuilderWrapper;

namespace CalabrioRetentionListener.Services
{
    public class ConsumerBuilderWrapper : IConsumerBuilderWrapper
    {
        public IConsumer<string, string> Build(ConsumerConfig consumerConfig)
        {
            return new ConsumerBuilder<string, string>(consumerConfig).Build();
        }
    }
}
