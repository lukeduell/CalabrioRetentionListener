using CalabrioRetentionListener.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CalabrioRetentionListener.Services
{
    public interface IKafkaAckProducerService: IDisposable
    {
        Task SendAcknowledgementMessageAsync(AcknowledgementMessage ackMessage);
    }
}
