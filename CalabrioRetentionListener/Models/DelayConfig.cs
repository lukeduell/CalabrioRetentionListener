using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CalabrioRetentionListener.Models
{
    public class DelayConfig
    {
        public TimeSpan RefreshKafkaSubscriptionDelay { get; set; }
    }
}
