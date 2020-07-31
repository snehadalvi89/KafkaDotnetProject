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

namespace KafkaProject.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class  ProducerController : ControllerBase
    {
         private  ProducerConfig _producerconfig;

         // private readonly IConfiguration _configuration;
        public ProducerController(ProducerConfig config)
        {
            _producerconfig=config;
        }

  
        [HttpPost("{topic}")]
       
        public async Task<ActionResult> Create(string topic,[FromBody]String message)
        {
             using(var producer=new ProducerBuilder<Null,String>(_producerconfig).Build())
           {
               await producer.ProduceAsync("NewTopic", new Message<Null, string>(){Value=message});
               producer.Flush(TimeSpan.FromSeconds(10));
               return Ok(true);

           }
        }

        
        
    }
}
