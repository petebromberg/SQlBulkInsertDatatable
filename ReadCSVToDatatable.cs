using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.IO;
using System.Text.RegularExpressions;

namespace SQlBulkInsertDatatable
{
    public static class Convert
    {
        public static DataTable ConvertCSVToDataTable(string strFilePath)
        {
            StreamReader sr = new StreamReader(strFilePath);
            string[] headers = sr.ReadLine().Split(',');
            DataTable dt = new DataTable();
            foreach (string header in headers)
            {
                dt.Columns.Add(header);
            }
            while (!sr.EndOfStream)
            {
                string[] rows = Regex.Split(sr.ReadLine(), ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                DataRow dr = dt.NewRow();
                for (int i = 0; i < headers.Length; i++)
                {
                    dr[i] = rows[i];
                }
                dt.Rows.Add(dr);
            }
            return dt;
        }


        public static DataTable CopyRows(this DataTable from, DataTable to, int min, int max)
        {
            for (int i = min; i < max && i < from.Rows.Count; i++)
                to.ImportRow(from.Rows[i]);
                    return to;

        }
    }
}
