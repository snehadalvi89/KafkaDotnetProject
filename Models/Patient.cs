using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace KafkaProject.Models
{
public class Patient
{
        public int Patientid {get;set;}

        public string PatientName {get;set;}

        public int InsuranceId {get;set;}

        public string PatientIllness {get;set;}

        public DateTime PatientAdmissionDate {get;set;}

        public DateTime PatientEntryDate {get;set;}

        public String Treatments {get;set;}

        public int Medicinevalue {get;set;}

        public int TreatmentValue {get;set;}

}
}