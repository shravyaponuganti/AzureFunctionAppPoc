using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using CsvHelper;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace AzureFunctionAppPoc
{
    public class BlobReader
    {
        readonly static string connectionString = "Endpoint=sb://sb-ipaas-sbx-euno-01.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=KA15Q9XVzIKzutc/fxnOUdR4eRpqz5gW6degt/M2EmM=";
        readonly static string queueName = "az-queue";

        [FunctionName("BlobReader")]
        public async Task Run([BlobTrigger("azure-blob-storagecontainer/{name}", Connection = "ConnectionSA")] Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");

            //Only convert CSV files
            if (name.Contains(".csv"))
            {
                log.LogInformation("Converting to JSON");
                var json = Convert(myBlob);

                await SendtoQueue(json);
                log.LogInformation($"C# ServiceBus queue trigger function processed message to (az-queue) : {json}");

                //var fileName = name.Replace(".csv", "");
                //log.LogInformation($"Creating {fileName}.json");
                //CreateJSONBlob(json, fileName);

                //Uncomment this to see JSON in the console
                //log.LogInformation(json); 
            }
            else
            {
                log.LogInformation("Not a CSV");
            }
        }

        public async static Task SendtoQueue(string json)
        {
            await using var client = new ServiceBusClient(connectionString);

            // create the sender
            ServiceBusSender sender = client.CreateSender(queueName);

            // create a message that we can send. UTF-8 encoding is used when providing a string.
            ServiceBusMessage message = new ServiceBusMessage(json);
            // send the message
            await sender.SendMessageAsync(message);
        }
        public static string Convert(Stream blob)
        {
            var csv = new CsvReader(new StreamReader(blob), CultureInfo.InvariantCulture);
            csv.Read();
            csv.ReadHeader();
            var csvRecords = csv.GetRecords<object>().ToList();

            //Convert to JSON
            return JsonConvert.SerializeObject(csvRecords);
        }
    }
}
