using System;
using System.Data;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;
using System.Collections.Generic;

namespace SQlBulkInsertDatatable
{
    class Program
    {
        static float loadTime = 0;
        static float insertTime = 0;
        static void Main(string[] args)
        {

            Go();
        }


        static async void Go()
        {

            Stopwatch s = new Stopwatch();
            s.Start();
            string path = @"C:\TEMP\cSun_tSSB_6000y_x5d.tsv\cSun_tSSB_6000y_x5d.csv";
            DataTable dt = Convert.ConvertCSVToDataTable(path);
            s.Stop();
            loadTime = s.ElapsedMilliseconds;

            int rowsPerTable = dt.Rows.Count / 4;

            // split up DataTable to 4 separa chunks for parallel testing
            DataTable dt1 = new DataTable();
            DataTable dt2 = new DataTable();
            DataTable dt3 = new DataTable();
            DataTable dt4 = new DataTable();
            Convert.CopyRows(dt, dt1, 0, rowsPerTable);
            Convert.CopyRows(dt, dt2, rowsPerTable, rowsPerTable * 2);
            Convert.CopyRows(dt, dt3, rowsPerTable * 2, rowsPerTable * 3);
            Convert.CopyRows(dt, dt4, rowsPerTable * 3, rowsPerTable * 4);


            s.Reset();
            var tasks = new List<Task>();
            tasks.Add(Task.Run(() => Execute(dt1)));
            tasks.Add(Task.Run(() => Execute(dt2)));
            tasks.Add(Task.Run(() => Execute(dt3)));
            tasks.Add(Task.Run(() => Execute(dt4)));

            s.Start();
           
            Task t = Task.WhenAll(tasks);
            try
            {
                t.Wait();
            }
            catch { }

            s.Stop();
            insertTime = s.ElapsedMilliseconds;
            Console.WriteLine("Read in " + loadTime.ToString() + "ms  and  inserted " + dt.Rows.Count.ToString() + " rows to database in " + insertTime.ToString() + "ms.");
        }

 

        public static async Task Execute(DataTable dt)
        {
            string connStr = ConfigurationManager.ConnectionStrings["Default"].ConnectionString;
          
            SqlConnection conn = new SqlConnection(connStr);
            //features tablock
            conn.Open();
            try
            {
                using (SqlBulkCopy sbc = new SqlBulkCopy(conn))
                {
                    
                    sbc.DestinationTableName = "dbo.CSV";
                    sbc.BulkCopyTimeout = 0;
                    sbc.BatchSize = dt.Rows.Count;
                    await sbc.WriteToServerAsync(dt);
                    sbc.Close();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);

            }
            
        }
    }
         

         
    }

