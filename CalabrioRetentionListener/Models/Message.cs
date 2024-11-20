using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CalabrioRetentionListener.Models
{
    public class Message<T>
    {
        public string Body { get; set; }
        public T CommitToken { get; set; }
    }
}
