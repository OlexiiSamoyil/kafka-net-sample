namespace KafkaSampleCommon
{
    public static class KafkaConfig
    {
        public static string TopicName { get; } = "dotnet_community_topic";
        public static string BootstrapServers { get; } = "localhost:9092";
        public static string ConsumerGroup { get; } = "string_messages_consumer_group3";
    }
}
