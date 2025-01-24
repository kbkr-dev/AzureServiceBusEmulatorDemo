using Azure.Messaging.ServiceBus;
using System.Text;

namespace Publisher
{
    internal class Program
    {
        private static string _connectionString = "Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";
        static async Task Main(string[] args)
        {
            await PublishMessageToQueue();
            await PublishMessageToTopic();
        }

        private static async Task PublishMessageToQueue()
        {
            const int numOfMessagesPerBatch = 10;
            const int numOfBatches = 10;

            string queueName = "queue.1";

            var client = new ServiceBusClient(_connectionString);
            var sender = client.CreateSender(queueName);

            for (int i = 1; i <= numOfBatches; i++)
            {
                using ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync();

                for (int j = 1; j <= numOfMessagesPerBatch; j++)
                {
                    messageBatch.TryAddMessage(new ServiceBusMessage($"Batch:{i}:Message:{j}, sample message"));
                }
                await sender.SendMessagesAsync(messageBatch);
            }

            await sender.DisposeAsync();
            await client.DisposeAsync();

            Console.WriteLine($"{numOfBatches} batches with {numOfMessagesPerBatch} messages per batch has been published to the queue.");
        }

        private static async Task PublishMessageToTopic()
        {
            var topicName = "topic.1";

            await using (var client = new ServiceBusClient(_connectionString))
            {
                ServiceBusSender sender = client.CreateSender(topicName);

                //First 50 message will goto Subscription 1 and Subscription 3 as per set filters in Config.json
                for (int i = 1; i <= 50; i++)
                {
                    ServiceBusMessage message = new ServiceBusMessage(Encoding.UTF8.GetBytes($"Message number : {i}"))
                    {
                        ContentType = "application/json"
                    };

                    await sender.SendMessageAsync(message);
                }

                //Next 50 message will goto Subscription 2 and Subscription 3 as per set filters  in Config.json

                for (int i = 51; i <= 100; i++)
                {
                    ServiceBusMessage message = new ServiceBusMessage(Encoding.UTF8.GetBytes($"Message number : {i}"));
                    message.ApplicationProperties.Add("prop1", "value1");

                    await sender.SendMessageAsync(message);
                }
            }

            Console.WriteLine("Sent 100 messages to the topic.");
        }
    }
}
