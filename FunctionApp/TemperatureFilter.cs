using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Microsoft.Demo
{
    public static class TemperatureFilter
    {
        [FunctionName("TemperatureFilter")]
        public static async Task Run(
            [EventHubTrigger("iothub-messages", Connection = "EventHubCompatibleIoTHubEndpoint")] string messageString,
            [EventHub("dest", Connection = "OutputEventHubConnectionString")]IAsyncCollector<string> outputEvents,
            ILogger logger)
        {
            const int temperatureThreshold = 20;

            if (!string.IsNullOrEmpty(messageString))
            {
                logger.LogInformation("Info: Received one non-empty message");
                // Get the body of the message and deserialize it.
                var messageBody = JsonConvert.DeserializeObject<MessageBody>(messageString);

                if (messageBody != null && messageBody.machine.temperature > temperatureThreshold)
                {
                    // Send the message to the output as the temperature value is greater than the threshold.
                    var filteredMessage = new MessageBody() {
                        timeCreated = messageBody.timeCreated,
                        ambient = messageBody.ambient,
                        machine = messageBody.machine
                    };
                    filteredMessage.messageType = "Alert";
                    await outputEvents.AddAsync(JsonConvert.SerializeObject(filteredMessage));
                    logger.LogInformation("Info: Received and transferred a message with temperature above the threshold");
                }
            }
        }
    }
    //Define the expected schema for the body of incoming messages.
    class MessageBody
    {
        public Machine machine { get; set; }
        public Ambient ambient { get; set; }
        public string timeCreated { get; set; }
        public string messageType { get; set; }
    }
    class Machine
    {
        public double temperature { get; set; }
        public double pressure { get; set; }
    }
    class Ambient
    {
        public double temperature { get; set; }
        public int humidity { get; set; }
    }
}
