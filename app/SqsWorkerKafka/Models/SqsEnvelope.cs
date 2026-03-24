using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace SqsWorkerKafka.Models;

public sealed record SqsEnvelope(
    [property: JsonPropertyName("partitionKey")] string PartitionKey,
    [property: JsonPropertyName("id")] string Id,
    [property: JsonPropertyName("timestamp")] long Timestamp);