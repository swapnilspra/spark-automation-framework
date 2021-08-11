using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.IO;
using Npgsql;



namespace mdGenerator
{
    public partial class Form1 : Form
    {
        NpgsqlConnection olconn;
        private DataSet ds = new DataSet();
        private DataTable dtTemp = new DataTable();
        private DataTable dtTables = new DataTable();
        private DataTable dtCols = new DataTable();
        private string strTableColsQuery = "SELECT '|[' || column_name || '](#' || column_name || ')|' || data_type || '|' || character_maximum_length || '|' || is_nullable || '|' || coalesce(column_default,'') || '|'" +
                                            " FROM information_schema.columns" +
                                            " WHERE table_schema = 'public' AND table_name = '{0}' and data_type in ('character','character varying')" +
                                            " union all" +
                                            " SELECT '|[' || column_name || '](#' || column_name || ')|' || data_type || '|(' || numeric_precision || ',' ||  numeric_scale || ')|' || is_nullable || '|' || coalesce(column_default,'') || '|'" +
                                            " FROM information_schema.columns" +
                                            " WHERE table_schema = 'public' AND table_name = '{0}' and data_type in ('integer','numeric','smallint')" +
                                            " union all" +
                                            " SELECT '|[' || column_name || '](#' || column_name || ')|' || data_type || '|' || datetime_precision || '|' || is_nullable || '|' || coalesce(column_default,'') || '|'" +
                                            " FROM information_schema.columns" +
                                            " WHERE table_schema = 'public' AND table_name = '{0}' and data_type in ('timestamp without time zone')" +
                                            " union all" +
                                            " SELECT '|[' || column_name || '](#' || column_name || ')|' || data_type || '| 1GB |' || is_nullable || '|' || coalesce(column_default,'') || '|'" +
                                            " FROM information_schema.columns" +
                                            " WHERE table_schema = 'public' AND table_name = '{0}' and data_type in ('bytea','text')";


        public Form1()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e)
        {

        }

        private void button1_Click(object sender, EventArgs e)
        {
            string strConnstring = String.Format("Server={0};Port={1};" +
                    "User Id={2};Password={3};Database={4};",
                    txtHost.Text, txtPort.Text, txtUser.Text,
                    txtPass.Text, txtService.Text);

            // Making connection with Npgsql provid
            olconn = new NpgsqlConnection(strConnstring);

            try
            {
                olconn.Open();
                olconn.Close();
                MessageBox.Show("Connected");
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        private string isPk (string tblname, string colname)
        {
            NpgsqlCommand comm = new NpgsqlCommand("select case when count(*) > 0 then 'YES' else 'NO' end from information_schema.table_constraints "+ 
                                                    " tco join information_schema.key_column_usage kcu  on kcu.constraint_name = tco.constraint_name " +
                                                    " and kcu.constraint_schema = tco.constraint_schema and kcu.constraint_name = tco.constraint_name " + 
                                                    " where tco.constraint_type = 'PRIMARY KEY' and kcu.table_name = '" + tblname + "' and " + 
                                                    "kcu.column_name = '" + colname + "'", olconn);
            return comm.ExecuteScalar().ToString();
        }

        private DataTable getResults(string SQL)
        {
            NpgsqlDataAdapter da = new NpgsqlDataAdapter(SQL, olconn);
            ds = new DataSet();
            da.Fill(ds);
            return ds.Tables[0];
        }

        private void WriteColsSection (StreamWriter sw, string ColName)
        {
            sw.WriteLine("### " + ColName);
            sw.WriteLine("#### Description");
            sw.WriteLine("");
            sw.WriteLine("Lorem ipsum dolor sit amet");
            sw.WriteLine("");
            sw.WriteLine("#### Value Range");
            sw.WriteLine("");
            sw.WriteLine("N/A");
            sw.WriteLine("");
            sw.WriteLine("#### Logic");
            sw.WriteLine("");
            sw.WriteLine("Hybrid of sequence generator and source value from HDM.FUND id.");
            sw.WriteLine("");
            sw.WriteLine("```");
            sw.WriteLine("Autoincrement +1 for new inserts");
            sw.WriteLine("```");
            sw.WriteLine("");
            sw.WriteLine("");
            sw.WriteLine("");
        }

        private void button2_Click(object sender, EventArgs e)
        {
            strTableColsQuery = "SELECT '|[' || column_name || '](#' || column_name || ')|' || data_type || '|' || character_maximum_length || '|' || is_nullable || '|' || coalesce(column_default,'') || '|'" +
                                            " FROM information_schema.columns" +
                                            " WHERE table_schema = '" + txtSchema.Text +"' AND table_name = '{0}' and data_type in ('character','character varying')" +
                                            " union all" +
                                            " SELECT '|[' || column_name || '](#' || column_name || ')|' || data_type || '|(' || numeric_precision || ',' ||  numeric_scale || ')|' || is_nullable || '|' || coalesce(column_default,'') || '|'" +
                                            " FROM information_schema.columns" +
                                            " WHERE table_schema = '" + txtSchema.Text + "' AND table_name = '{0}' and data_type in ('integer','numeric','smallint')" +
                                            " union all" +
                                            " SELECT '|[' || column_name || '](#' || column_name || ')|' || data_type || '|' || datetime_precision || '|' || is_nullable || '|' || coalesce(column_default,'') || '|'" +
                                            " FROM information_schema.columns" +
                                            " WHERE table_schema = '" + txtSchema.Text + "' AND table_name = '{0}' and data_type in ('timestamp without time zone')" +
                                            " union all" +
                                            " SELECT '|[' || column_name || '](#' || column_name || ')|' || data_type || '| 1GB |' || is_nullable || '|' || coalesce(column_default,'') || '|'" +
                                            " FROM information_schema.columns" +
                                            " WHERE table_schema = '" + txtSchema.Text + "' AND table_name = '{0}' and data_type in ('bytea','text')";

            string strConnstring = String.Format("Server={0};Port={1};" +
                    "User Id={2};Password={3};Database={4};",
                    txtHost.Text, txtPort.Text, txtUser.Text,
                    txtPass.Text, txtService.Text);

            // Making connection with Npgsql provid
            olconn = new NpgsqlConnection(strConnstring);

            try
            {
                olconn.Open();

                dtTables = getResults("select table_name from information_schema.tables where table_schema =lower('" + txtSchema.Text +
                    "') and table_type='BASE TABLE'");
                
                if (!Directory.Exists("all mds"))
                {
                    Directory.CreateDirectory(Directory.GetCurrentDirectory() + "\\all mds");
                }

                foreach (DataRow CurrDR in dtTables.Rows)
                {
                    StreamWriter sw = File.CreateText(Directory.GetCurrentDirectory() + "\\all mds\\" + txtSchema.Text +"-" + CurrDR[0] + ".md");

                    sw.WriteLine("# " + CurrDR[0].ToString().ToUpper());
                    sw.WriteLine("");
                    sw.WriteLine("This table holds the lorem ipsum dolor sit amet.");
                    sw.WriteLine("## DDL");
                    sw.WriteLine("");
                    sw.WriteLine("|Column Name |SQL Type |Length |Nullable |Default Value |PK |");
                    sw.WriteLine("|---        |---     |---   |---   |--- |--- |");

                    dtTemp = getResults(String.Format(strTableColsQuery, CurrDR[0].ToString()));

                    foreach (DataRow drCol in dtTemp.Rows)
                    {
                        sw.WriteLine(drCol[0] + isPk(CurrDR[0].ToString(), drCol[0].ToString().Split(']')[0].Substring(2)));
                    }

                    dtCols = getResults("SELECT column_name FROM information_schema.columns WHERE table_schema = lower('" + txtSchema.Text +
                    "') AND table_name = '" + CurrDR[0] + "'");

                    foreach (DataRow currCol in dtCols.Rows)
                    {
                        WriteColsSection(sw, currCol[0].ToString());
                    }

                    sw.Close();
                }

                olconn.Close();
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }
    }
}
