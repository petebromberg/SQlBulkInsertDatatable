using System;
using System.Data;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;

namespace SQlBulkInsertDatatable
{
    class Program
    {
        static void Main(string[] args)
        {
            Stopwatch s = new Stopwatch();
            s.Start();
            string path = @"C:\TEMP\cSun_tSSB_6000y_x5d.tsv\cSun_tSSB_6000y_x5d.csv";
            DataTable dt = Convert.ConvertCSVToDataTable(path);
            Task.Run(async () => await Execute(dt)).Wait();
            s.Stop();
            Console.WriteLine("Read and  inserted " + dt.Rows.Count.ToString() + " rows to database in " + s.ElapsedMilliseconds + " ms.");
        }

        public static async Task Execute(DataTable dt)
        {
            string connStr = ConfigurationManager.ConnectionStrings["Default"].ConnectionString;
            System.Threading.Tasks.Task<int> t;
            SqlConnection conn = new SqlConnection(connStr);
            conn.Open();
            try
            {
                using (SqlBulkCopy sbc =
               new SqlBulkCopy(conn))
                {
                    sbc.DestinationTableName = "dbo.CSV";
                    sbc.BulkCopyTimeout = 0;
                    sbc.BatchSize = dt.Rows.Count;
                    await sbc.WriteToServerAsync(dt);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);

            }
            
        }
    }
         

         
    }

