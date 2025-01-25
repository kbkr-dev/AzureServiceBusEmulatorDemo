using Azure.Messaging.ServiceBus;
using System.Text;

namespace Publisher
{
    internal class Program
    {
        private static string _connectionString = "Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";
        static async Task Main(string[] args)
        {
            string messageType = string.Empty;
            do
            {
                Console.WriteLine("Do you want to publish messages to a queue or a topic? (queue/topic), Enter quit or exit to close: ");
                messageType = Console.ReadLine();
                string message = string.Empty;
                switch (messageType)
                {
                    case "queue":
                        Console.WriteLine("Enter the queue name to publish messages: ");
                        string queueName = Console.ReadLine();
                        Console.WriteLine("Enter the message: ");
                        message = Console.ReadLine();
                        await PublishMessageToQueue(queueName, message);
                        break;
                    case "topic":
                        Console.WriteLine("Enter the topic name to publish messages: ");
                        string topicName = Console.ReadLine();
                        Console.WriteLine("Enter the message: ");
                        message = Console.ReadLine();
                        await PublishMessageToTopic(topicName, message);
                        break;
                    default:
                        Console.WriteLine("Invalid input. Please enter queue or topic.");
                        break;
                }
            } while (messageType.ToLower() != "quit" || messageType.ToLower() != "exit");
        }

        private static async Task PublishMessageToQueue(string queueName, string message)
        {
            var client = new ServiceBusClient(_connectionString);
            var sender = client.CreateSender(queueName);

            await sender.SendMessageAsync(new ServiceBusMessage(Encoding.UTF8.GetBytes(message)));

            await sender.DisposeAsync();
            await client.DisposeAsync();

        }

        private static async Task PublishMessageToTopic(string topicName, string message)
        {
            await using (var client = new ServiceBusClient(_connectionString))
            {
                ServiceBusSender sender = client.CreateSender(topicName);
                await sender.SendMessageAsync(new ServiceBusMessage(Encoding.UTF8.GetBytes($"Message for Subscription 1, 3 : {message}"))
                {
                    ContentType = "application/json"
                });

                for (int i = 51; i <= 100; i++)
                {
                    ServiceBusMessage sbMessage = new ServiceBusMessage(Encoding.UTF8.GetBytes($"Message for Subscription 2, 3 : {message}"));
                    sbMessage.ApplicationProperties.Add("prop1", "value1");

                    await sender.SendMessageAsync(sbMessage);
                }
            }
        }
    }
}
