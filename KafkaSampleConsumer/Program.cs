using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using KafkaSampleCommon;

namespace KafkaSampleConsumer
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Consumer has started");

            var conf = new ConsumerConfig
            {
                GroupId = KafkaConfig.ConsumerGroup,
                BootstrapServers = KafkaConfig.BootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            using var consumer = new ConsumerBuilder<int, string>(conf).Build();

            consumer.Subscribe(KafkaConfig.TopicName);
            var cancelToken = new CancellationTokenSource();

            Console.CancelKeyPress += (sender, eventArgs) =>
            {
                cancelToken.Cancel();
            };

            try
            {
                while (!cancelToken.IsCancellationRequested)
                {
                    var result = consumer.Consume(cancelToken.Token);
                    Console.WriteLine($"Message: {result.Message.Value}, key: {result.Message.Key}, received from {result.TopicPartitionOffset}, timestamp {result.Message.Timestamp.UtcDateTime}");
                    Console.Write("Topic assignments: ");
                    foreach (var consumerAssignment in GetConsumerAssignments(consumer))
                    {
                        Console.WriteLine(consumerAssignment);
                    }
                }
            }
            catch (Exception)
            {
                consumer.Close();
            }
        }

        private static IEnumerable<string> GetConsumerAssignments(IConsumer<int, string> consumer)
        {
            var assignments = 
                consumer.Assignment.GroupBy(a => a.Topic, b => b.Partition.Value);

            return assignments.Select(a => $"{a.Key} {string.Join(',', a)}");
        }
    }

   
}
