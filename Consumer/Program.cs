using Azure.Messaging.ServiceBus;
using System.Text;

namespace Consumer
{
    internal class Program
    {
        private static string _connectionString = "Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

        static async Task Main(string[] args)
        {
            await ConsumeQueue();
            await ConsumeTopic();
        }

        private static async Task ConsumeTopic()
        {
            var topicName = "topic.1";
            await ConsumeMessageFromSubscription(topicName, "subscription.1");
            await ConsumeMessageFromSubscription(topicName, "subscription.2");
            await ConsumeMessageFromSubscription(topicName, "subscription.3");
        }

        private static async Task ConsumeMessageFromSubscription(string topicName, string subscriptionName)
        {
            Console.WriteLine($"Rcv_Sub {subscriptionName} Begin");

            var client1 = new ServiceBusClient(_connectionString);
            var opt1 = new ServiceBusProcessorOptions();
            opt1.ReceiveMode = ServiceBusReceiveMode.PeekLock;
            var processor1 = client1.CreateProcessor(topicName, subscriptionName, opt1);

            processor1.ProcessMessageAsync += MessageHandler;
            processor1.ProcessErrorAsync += ErrorHandler;

            await processor1.StartProcessingAsync();

            await Task.Delay(TimeSpan.FromSeconds(5));

            await processor1.StopProcessingAsync();
            await processor1.DisposeAsync();
            await client1.DisposeAsync();
            Console.WriteLine($"Rcv_Sub {subscriptionName} End");
        }

        private static async Task MessageHandler(ProcessMessageEventArgs args)
        {
            string body = args.Message.Body.ToString();
            Console.WriteLine($"Received message: SequenceNumber:{args.Message.SequenceNumber} Body:{body}");
            await args.CompleteMessageAsync(args.Message);
        }

        private static Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine($"Message handler encountered an exception {args.Exception}.");
            return Task.CompletedTask;
        }

        private static async Task ConsumeQueue()
        {
            string queueName = "queue.1";

            var client = new ServiceBusClient(_connectionString);

            ServiceBusReceiverOptions opt = new ServiceBusReceiverOptions
            {
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
            };

            ServiceBusReceiver receiver = client.CreateReceiver(queueName, opt);

            while (true)
            {
                ServiceBusReceivedMessage message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
                if (message != null)
                {
                    Console.WriteLine($"Received message: {Encoding.UTF8.GetString(message.Body)}");

                    await receiver.CompleteMessageAsync(message);
                }
                else
                {
                    Console.WriteLine("No messages received.");
                    break;
                }
            }

            Console.WriteLine("Done recieving");

            await receiver.DisposeAsync();
            await client.DisposeAsync();
        }
    }
}
