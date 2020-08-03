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



        

        [HttpGet]
        public IActionResult Get()
        {
            //join code
            List<Employee> employees = new List<Employee>();
            List<Department> department = new List<Department>();
            string filename1 = "employee" + System.DateTime.Now.ToString("ddMMyyyy") + ".json";
            using (StreamReader r = new StreamReader(@"C:\Users\snehad2\Desktop\Kafka\Files\Employee\" + filename1))
            {
                string json = r.ReadToEnd();
                employees = JsonConvert.DeserializeObject<List<Employee>>(json);
                //var items = JsonConvert.DeserializeObject<Employee>(json);
            }

            //string filename2 = "department" + System.DateTime.Now.ToString("ddMMyyyy") + ".json";
            //using (StreamReader r = new StreamReader(@"C:\Users\snehad2\Desktop\Kafka\Files\Department\" + filename2))
            //{
            //    string json = r.ReadToEnd();
            //    department = JsonConvert.DeserializeObject<List<Department>>(json);
            //    //var items = JsonConvert.DeserializeObject<Employee>(json);
            //}

            //var data=( from emp in employees
            //join dep in department
            //on emp.DepartmentId equals dep.DepartmentId 

            //           select new { emp.EmployeeID,emp.FirstName,emp.LastName, dep.DepartmentName });

            //string matchedDataOutput= Newtonsoft.Json.JsonConvert.SerializeObject(data);


            //var notMatchedData =
            //    from e in employees
            //    where !data.Any(x => x.EmployeeID == e.EmployeeID)
            //    select e;

            //string unmatchedDataOutput = Newtonsoft.Json.JsonConvert.SerializeObject(notMatchedData);

            //string filename3 = "Logs" + System.DateTime.Now.ToString("ddMMyyyy") + ".json";
            ////string output = Newtonsoft.Json.JsonConvert.SerializeObject(cr.Message.Value, Newtonsoft.Json.Formatting.None);
            //System.IO.File.WriteAllText(@"C:\Users\snehad2\Desktop\Kafka\Files\Logs\" + filename3, matchedDataOutput);


            string result = string.Empty;
            var producerconfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };


            //using (var producer = new ProducerBuilder<Null, String>(producerconfig).Build())
            //{
            //    producer.ProduceAsync("NewTopic", new Message<Null, string>() { Value = matchedDataOutput });
            //    producer.Flush(TimeSpan.FromSeconds(10));
            //}


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
