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
using KafkaProject.Models;
using Newtonsoft.Json;
using System.Net.Http;
using Microsoft.AspNetCore.Http;
using System.Net;
using Microsoft.AspNetCore.Identity;
using System.IO;
using Newtonsoft.Json.Schema;
using Newtonsoft.Json.Linq;

namespace KafkaProject.Controllers
{
    [ApiController]
    [Route("[controller]")]


    public class ConsumerController : ControllerBase
    {
        private ConsumerConfig _consumerconfig;
        public ConsumerController(ConsumerConfig _config)
        {
            _consumerconfig = _config;
        }


        public void MergedData()
        {
         


            List<dynamic> data1;
            List<dynamic> data2;
           

            //join code
            bool data1valid = false;
            bool data2valid = false;
            string serializedmergeddata = String.Empty;
            string filename1 = "employee" + System.DateTime.Now.ToString("ddMMyyyy") + ".json";
            using (StreamReader r = new StreamReader(@"C:\Users\snehad2\Desktop\Kafka\Files\Employee\" + filename1))
            {
                string json = r.ReadToEnd();
                data1 = JsonConvert.DeserializeObject<List<dynamic>>(json);
                JSchema schema = JSchema.Parse(System.IO.File.ReadAllText("Employee.json"));
                JArray emparray = JArray.Parse(json);
                //JObject jsonobject = JObject.Parse(j.ToString());
                 data1valid = emparray.IsValid(schema);
            }

            string filename2 = "department" + System.DateTime.Now.ToString("ddMMyyyy") + ".json";
            using (StreamReader r = new StreamReader(@"C:\Users\snehad2\Desktop\Kafka\Files\Department\" + filename2))
            {
                string json = r.ReadToEnd();
                data2 = JsonConvert.DeserializeObject<List<dynamic>>(json);
                //JSchema schema = JSchema.Parse(System.IO.File.ReadAllText("department.json"));
                //JArray deparray = JArray.Parse(json);
                //data2valid= deparray.IsValid(schema);
                data2valid = true;
            }

            if (data1valid == true && data2valid == true)
            {
                var data3 = (from d1 in data1
                             join d2 in data2
                             on d1.DepartmentId equals d2.DepartmentId
                             select new { d1.EmployeeID, d1.FirstName, d1.LastName, d2.DepartmentName });

                serializedmergeddata = Newtonsoft.Json.JsonConvert.SerializeObject(data3);
            }
           

            // string filename3 = "Logs" + System.DateTime.Now.ToString("ddMMyyyy") + ".json";
            //string output = Newtonsoft.Json.JsonConvert.SerializeObject(cr.Message.Value, Newtonsoft.Json.Formatting.None);
            //System.IO.File.WriteAllText(@"C:\Users\snehad2\Desktop\Kafka\Files\Logs\" + filename3, hj);


            string result = string.Empty;
            var producerconfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };

            using (var producer = new ProducerBuilder<Null, String>(producerconfig).Build())
            {
                producer.ProduceAsync("NewTopic", new Message<Null, string>() { Value = serializedmergeddata });
                producer.Flush(TimeSpan.FromSeconds(10));
            }
        }
        

        [HttpGet]
        public IActionResult Get()
        {
            MergedData();

            string result = string.Empty;
            var producerconfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };

            using (var consumert = new ConsumerBuilder<Null, String>(_consumerconfig).Build())
            {
      
                List<string> topiclist = new List<string>();
                topiclist.Add("Employee");
                topiclist.Add("Department");
                consumert.Subscribe(topiclist);

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
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
                            var p = consumert.Name.ToString();

                            if (cr.Topic == "Employee")
                            {
                                string filename = "employee" + System.DateTime.Now.ToString("ddMMyyyy") + ".json";
                                string output = Newtonsoft.Json.JsonConvert.SerializeObject(cr.Message.Value);
                                System.IO.File.WriteAllText(@"C:\Users\snehad2\Desktop\Kafka\Files\Employee\" + filename, cr.Message.Value);
                            }
                            if (cr.Topic == "Department")
                            {
                                string filename = "department" + System.DateTime.Now.ToString("ddMMyyyy") + ".json";
                                //string output = Newtonsoft.Json.JsonConvert.SerializeObject(cr.Message.Value, Newtonsoft.Json.Formatting.None);
                                System.IO.File.WriteAllText(@"C:\Users\snehad2\Desktop\Kafka\Files\Department\" + filename, cr.Message.Value);
                            }


                            //to read data
                            //if (cr.Topic == "Employee")
                            //{
                            //    string filename = "employee" + System.DateTime.Now.ToString("ddMMyyyy") + ".json";
                            //    using (StreamReader r = new StreamReader(@"C:\Users\snehad2\Desktop\Kafka\Files\Employee\" + filename))
                            //    {
                            //        string json = r.ReadToEnd();
                            //        var t = JsonConvert.DeserializeObject<List<Employee>>(json);
                            //    }
                            //}


                           
                            //consumert.Close();
                            //Console.WriteLine($"Consumed message '{cr.Message.Value}' at: '{cr.TopicPartitionOffset}'.");
                            // Patient patientData = new Patient();
                            //patientData = JsonConvert.DeserializeObject<Patient>(cr.Message.Value);
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
                                producer.ProduceAsync("NewTopic", new Message<Null, string>() { Value = cr.Message.Value });
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
