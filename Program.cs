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
        static float loadTime = 0;
        static float insertTime = 0;
        static void Main(string[] args)
        {
            Stopwatch s = new Stopwatch();
            s.Start();
            string path = @"C:\TEMP\cSun_tSSB_6000y_x5d.tsv\cSun_tSSB_6000y_x5d.csv";
            DataTable dt = Convert.ConvertCSVToDataTable(path);
            s.Stop();
            loadTime = s.ElapsedMilliseconds;
            s.Reset();
            s.Start();
            Task.Run(async () => await Execute(dt)).Wait();
            s.Stop();
            insertTime = s.ElapsedMilliseconds;
            Console.WriteLine("Read in " + loadTime.ToString() + "ms  and  inserted " + dt.Rows.Count.ToString() + " rows to database in " + insertTime.ToString() + "ms.");
        }


        /*public async Task Execute()
{
    using(var sourceConnection = new SqlConnection(_sourceConnectionString))
    using(var targetConnection = new SqlConnection(_targetConnectionString))
    using(var sourceCommand = new SqlCommand(_selectQuery, sourceConnection))
    using(var bulkCopy = new SqlBulkCopy(targetConnection))
    {
        await Task.WhenAll(sourceConnection.OpenAsync(),
                           targetConnection.OpenAsync());
 
        var reader = await sourceCommand.ExecuteReaderAsync();
 
        bulkCopy.DestinationTableName = _targetTable;
        await bulkCopy.WriteToServerAsync(reader);
    }
}
        */

        public static async Task Execute(DataTable dt)
        {
            string connStr = ConfigurationManager.ConnectionStrings["Default"].ConnectionString;

            SqlConnection conn = new SqlConnection(connStr);
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
