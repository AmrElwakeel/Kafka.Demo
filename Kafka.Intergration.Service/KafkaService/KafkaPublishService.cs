using Confluent.Kafka;
using System.Diagnostics;
using System.Net;

namespace Kafka.Intergration.Service.KafkaService
{
    public class KafkaPublishService
    {
        private readonly string _bootstrapServers = "localhost:9092";
        private readonly string _topic = "test";
        public KafkaPublishService() { }

        public async Task<bool> SendOrderRequest(string message)
        {
            ProducerConfig config = new()
            {
                BootstrapServers = _bootstrapServers,
                ClientId = Dns.GetHostName()
            };

            try
            {
                using var producer = new ProducerBuilder<Null, string>(config).Build();
                var result = await producer.ProduceAsync
                (_topic, new Message<Null, string>
                {
                    Value = message
                });

                Debug.WriteLine($"Delivery Timestamp:{result.Timestamp.UtcDateTime}");
                return await Task.FromResult(true);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error occured: {ex.Message}");
            }

            return await Task.FromResult(false);
        }
    }
}
