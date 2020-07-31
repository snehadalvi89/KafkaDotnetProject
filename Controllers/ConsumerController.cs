using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using static Confluent.Kafka.ConfigPropertyNames;
using  KafkaProject.Models;
using Newtonsoft.Json;
using System.Net.Http;
using Microsoft.AspNetCore.Http;
using System.Net;
using Microsoft.AspNetCore.Identity;

namespace KafkaProject.Controllers
{
    [ApiController]
    [Route("[controller]")]

  
    public class  ConsumerController : ControllerBase
    {
        private  ConsumerConfig _consumerconfig;
        public ConsumerController(ConsumerConfig _config)
        {
            _consumerconfig=_config;
        }
       




        [HttpGet]
        public IActionResult Get()
        {
            string result = string.Empty;
            var config = new ConsumerConfig()
            {
                GroupId = "gid-consumer",
                BootstrapServers = "localhost:9092"

            };
            var producerconfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };
            //var logFile = System.IO.File.Create("KafkaMessage");
            //var logWriter = new System.IO.StreamWriter(logFile);

            using (var consumert=new ConsumerBuilder<Null,String>(_consumerconfig).Build())
           {
             
           consumert.Subscribe("test");
           
           CancellationTokenSource cts = new CancellationTokenSource();
           Console.CancelKeyPress += (_, e) => {
                e.Cancel = false; // prevent the process from terminating.
                cts.Cancel();
            };

            try
            {
                while (true)
                {
                    try
                    {
                            var cr = consumert.Consume();
                            //consumert.Close();
                            //Console.WriteLine($"Consumed message '{cr.Message.Value}' at: '{cr.TopicPartitionOffset}'.");
                            Patient patientData = new Patient();
                            patientData = JsonConvert.DeserializeObject<Patient>(cr.Message.Value);
                            // Console.WriteLine("start");
                            // Console.WriteLine(p.PatientName+p.Patientid+p.InsuranceId+p.PatientAdmissionDate);
                            // Console.WriteLine("end");

                            //var logPath = System.IO.Path.GetFullPath(@"C:\Users\snehad2\Desktop\KafkaResponse");
                            //using (var logWriter = new System.IO.StreamWriter(logPath+@"\KafkaMessage.txt"))
                            //{

                            //    logWriter.WriteLine(cr.Message.Value);
                            //    logWriter.Dispose();
                            //}

                           
                            result = cr.Message.Value;
                            using (var producer = new ProducerBuilder<Null, String>(producerconfig).Build())
                            {
                                producer.ProduceAsync("NewTopic", new Message<Null, string>() { Value = cr.Message.Value + "123" });
                                producer.Flush(TimeSpan.FromSeconds(10));
                            }

                        }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                consumert.Close();
            }
                finally
                {
                   // logWriter.Dispose();
                }

           }
            return Ok(result);
        }
        
    }
}
