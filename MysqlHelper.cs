using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;
using System.Data;

namespace ZW.DbBasic
{
    public class MysqlHelper:DBHelper
    {

        public MysqlHelper(string strConn)
            : base(strConn)
         { }

        public override bool IsOpen(out string ErrorInfo)
        {
            using (MySqlConnection conn = new MySqlConnection(_StrConn))
            {
                try
                {
                    conn.Open();
                    conn.Close();
                    ErrorInfo = "";
                    return true;
                }
                catch(Exception ex)
                {
                    ErrorInfo = ex.Message;
                    return false;
                }

            }
        }

        public override int GetMaxId(string FieldName, string TableName)
        {
            string strSql = "select max(" + FieldName + ")+1 from " + TableName;
            object obj = GetSingle(strSql);
            if (obj == null)
            {
                return 1;
            }
            else
            {
                return int.Parse(obj.ToString());
            }
        }

        public override bool Exists(SqlEventArgs e)
        {
            object obj = GetSingle(e);
            if (Object.Equals(obj, null))
            {
                return false;
            }
            else
            {
                return true;
            }
        }

        public override object GetSingle(SqlEventArgs e)
        {
          
            MySqlCommand cmd = new MySqlCommand();
            using (MySqlConnection conn = new MySqlConnection(_StrConn))
            {
                try
                {
                    ParameterCommandAdd(cmd, conn, null, e.CommandType, e.Text, e.Parameters);
                    object obj = cmd.ExecuteScalar();
                    if (Object.Equals(obj, null) || Object.Equals(obj, DBNull.Value))
                    {
                        return null;
                    }
                    else
                    {
                        return obj;
                    }
                }
                catch (MySqlException ex)
                {
                    return null;
                }
                finally
                {
                    if (conn.State != ConnectionState.Closed)
                    {
                        conn.Close();
                    }
                    if (cmd.Parameters != null)
                    {
                        cmd.Parameters.Clear();
                    }
                }
            }
        }

        public override int Execute(SqlEventArgs e)
        {
            using (MySqlConnection conn = new MySqlConnection(_StrConn))
            {
                MySqlCommand cmd = new MySqlCommand();
                try
                {
                    ParameterCommandAdd(cmd, conn, null, e.CommandType, e.Text, e.Parameters);
                    return cmd.ExecuteNonQuery();
                }
                catch (MySqlException ex)
                {
                    return 0;
                }
                finally
                {
                    if (conn.State != ConnectionState.Closed)
                    {
                        conn.Close();
                    }
                    if (cmd.Parameters != null)
                    {
                        cmd.Parameters.Clear();
                    }
                }
            }
        }

        public override bool Execute(List<SqlEventArgs> e)
        {
            bool success = true;
            using (MySqlConnection conn = new MySqlConnection(_StrConn))
            {
                MySqlCommand cmd = new MySqlCommand();

                conn.Open();

                using (MySqlTransaction tran = conn.BeginTransaction())
                {
                    try
                    {

                        foreach (SqlEventArgs ea in e)
                        {
                            ParameterCommandAdd(cmd, conn, tran, ea.CommandType, ea.Text, ea.Parameters);
                            cmd.ExecuteNonQuery();
                        }

                    }
                    catch
                    {
                        tran.Rollback();
                        success = false;
                    }
                    finally
                    {
                        tran.Commit();

                        if (conn.State != ConnectionState.Closed)
                        {
                            conn.Close();
                        }
                    } 
                }
              
             
            }
            return success;
        }

        public override System.Data.Common.DbDataReader GetDataReader(SqlEventArgs e)
        {
            using (MySqlConnection conn = new MySqlConnection(_StrConn))
            {
                MySqlCommand cmd = new MySqlCommand();
                try
                {
                    ParameterCommandAdd(cmd, conn, null, e.CommandType, e.Text, e.Parameters);

                    MySqlDataReader read = cmd.ExecuteReader(CommandBehavior.CloseConnection);

                    return read;
                }
                catch (MySqlException ex)
                {
                    return null;
                }
                finally
                {
                    if (conn.State != ConnectionState.Closed)
                    {
                        conn.Close();
                    }
                    if (cmd.Parameters != null)
                    {
                        cmd.Parameters.Clear();
                    }

                }

            }
        }

        public override System.Data.DataSet GetDataSet(SqlEventArgs e)
        {    
            DataSet ds = new DataSet();
            using (MySqlConnection conn = new MySqlConnection(_StrConn))
            {
               
                try
                {
                    MySqlCommand cmd = new MySqlCommand();
                    ParameterCommandAdd(cmd, conn, null, e.CommandType, e.Text, e.Parameters);
                    MySqlDataAdapter adapter = new MySqlDataAdapter(cmd); 
                    adapter.Fill(ds);
                    return ds;
                }
                catch (MySqlException ex)
                {
                    return null;
                }
                finally
                {
                    if (conn.State != ConnectionState.Closed)
                    {
                        conn.Close();
                    }
                } 
            }
        }

        public override System.Data.DataRow GetDataRow(SqlEventArgs e)
        {
            DataSet ds = GetDataSet(e);
            if (ds != null)
            {
                if (ds.Tables[0].Rows.Count > 0)
                    return ds.Tables[0].Rows[0];
                else
                    return null;

            }
            else
            {
                return null;
            }
        }
    }
}
