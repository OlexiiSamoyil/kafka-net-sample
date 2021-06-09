using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaSampleCommon;

namespace KafkaSamplePublisher
{
    internal class Program
    {
        public static async Task Main(string[] args)
        {
            var key = 0;

            var producerConfig = new ProducerConfig
                {BootstrapServers = KafkaConfig.BootstrapServers};

            Console.WriteLine("Publisher has started");
            Console.WriteLine($"Send messages to {KafkaConfig.TopicName} topic:");

            using var producer = new ProducerBuilder<int, string>(producerConfig).Build();
            try
            {
                while (true)
                {
                    var message = Console.ReadLine();

                    var deliveryResult = await producer.ProduceAsync(KafkaConfig.TopicName,
                        new Message<int, string> {Value = message, Key = key++, });

                    Console.WriteLine(
                        $"Message has been published: partition {deliveryResult.Partition.Value}, offset {deliveryResult.Offset.Value}");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Oops, something went wrong: {e}");
            }
        }
    }
}
