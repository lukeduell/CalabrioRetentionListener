using System;
using Newtonsoft.Json;

namespace CalabrioRetentionListener.Models
{
    public class AcknowledgementMessage
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("correlationid")]
        public string CorrelationId { get; set; }

        [JsonProperty("source")]
        public string Source { get; set; }

        [JsonProperty("specversion")]
        public string SpecVersion { get; set; }

        [JsonProperty("type")]
        public string Type { get; set; }

        [JsonProperty("time")]
        public string Time { get; set; }

        [JsonProperty("datacontenttype")]
        public string DataContentType { get; set; }

        [JsonProperty("subject")]
        public string Subject { get; set; }

        [JsonProperty("data")]
        public AcknowledgementData Data { get; set; }
    }

    public class AcknowledgementData
    {
        [JsonProperty("acknowledgementId")]
        public string AcknowledgementId { get; set; }

        [JsonProperty("acknowledgementTimestamp")]
        public string AcknowledgementTimestamp { get; set; }

        [JsonProperty("actionTaken")]
        public string ActionTaken { get; set; } // Possible values: "deidentify", "deleted", "no action taken", "not found"
    }
}
