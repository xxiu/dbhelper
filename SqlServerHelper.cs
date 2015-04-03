using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;

namespace ZW.DbBasic
{
    public class SqlServerHelper:DBHelper
    {

          public SqlServerHelper(string strConn)
            : base(strConn)
         { }

          public override bool IsOpen(out string ErrorInfo)
          { 
              using (SqlConnection conn = new SqlConnection(_StrConn))
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
            SqlCommand cmd = new SqlCommand();
            using (SqlConnection conn = new SqlConnection(_StrConn))
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
                catch (SqlException ex)
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
            using (SqlConnection conn = new SqlConnection(_StrConn))
            {
                SqlCommand cmd = new SqlCommand();
                try
                {
                    ParameterCommandAdd(cmd, conn, null, e.CommandType, e.Text, e.Parameters);
                    return cmd.ExecuteNonQuery();
                }
                catch (SqlException ex)
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
            using (SqlConnection conn = new SqlConnection(_StrConn))
            {
                SqlCommand cmd = new SqlCommand();
                conn.Open();
                SqlTransaction tran = conn.BeginTransaction();
              
                try
                {
                    foreach (SqlEventArgs ea in e)
                    {
                        ParameterCommandAdd(cmd, conn, tran, ea.CommandType, ea.Text, ea.Parameters);
                        cmd.ExecuteNonQuery();
                    }
                    tran.Commit();

                }
                catch
                {
                    tran.Rollback();
                    success = false;
                }
                finally
                {
                    if (conn.State != ConnectionState.Closed)
                    {
                        conn.Close();
                    }
                }
            }
            return success;
        }

        public override System.Data.Common.DbDataReader GetDataReader(SqlEventArgs e)
        {
            using (SqlConnection conn = new SqlConnection(_StrConn))
            {
                SqlCommand cmd = new SqlCommand();
                try
                {
                    ParameterCommandAdd(cmd, conn, null, e.CommandType, e.Text, e.Parameters);

                    SqlDataReader read = cmd.ExecuteReader(CommandBehavior.CloseConnection);

                    return read;
                }
                catch (SqlException ex)
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
            using (SqlConnection conn = new SqlConnection(_StrConn))
            {
                DataSet ds = new DataSet();
                try
                {
                    SqlCommand cmd = new SqlCommand();
                    ParameterCommandAdd(cmd, conn, null, e.CommandType, e.Text, e.Parameters);
                    SqlDataAdapter adapter = new SqlDataAdapter(cmd);
                    adapter.Fill(ds, "ds");
                }
                catch (SqlException ex)
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
                return ds;
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
