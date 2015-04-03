 using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data ;
using System.Data .Common ;
using System.Collections;
 
namespace ZW.DbBasic
{
    public abstract class DBHelper
    {
         /// <summary>
        /// 数据库连接字符串
        /// </summary>
        protected  string _StrConn;

        public string StrConn
        {
            set { _StrConn = value; }
        }
        public DBHelper(string strConn)
        {
            this._StrConn = strConn;
        }

        #region DBHelper 成员

        /// <summary>
        /// 判断数据库是否能够链接
        /// </summary>
        /// <returns></returns>
        public abstract bool IsOpen(out string ErrorInfo);

        /// <summary>
        /// 查找表的最大id ，主键必须为int型
        /// </summary>
        /// <param name="FieldName">主键</param>
        /// <param name="TableName">表名</param>
        /// <returns></returns>
        public abstract int GetMaxId(string FieldName, string TableName);
         
         
        /// <summary>
        /// 执行一个sql语句，判断该语句是否存在
        /// </summary>
        /// <param name="strSql"></param>
        /// <returns></returns>
       
       
        public bool Exists(string strSql)
        {
            return Exists(strSql,CommandType.Text);
        } 
        public bool Exists(string strSql, CommandType commandType)
        {
            return Exists(strSql, commandType, null); 
        }
        public bool Exists(string strSql, CommandType commandType, DbParameter[] cmdParms)
        {
            SqlEventArgs e = new SqlEventArgs() { Text=strSql, CommandType=commandType, Parameters=cmdParms };
            return Exists(e);
        } 
        public abstract bool Exists(SqlEventArgs e);
        


        /// <summary>
        /// 执行一条计算查询结果的语句，返回查询结果
        /// </summary>
        /// <param name="strSql"></param>
        /// <returns></returns> 
       
        public object GetSingle(string strSql)
        {
            return GetSingle(strSql, CommandType.Text);
        }
        public object GetSingle(string strSql, CommandType commandType)
        {
            return GetSingle(strSql, commandType,null);
        }
        public object GetSingle(string strSql, CommandType commandType, DbParameter[] cmdParms)
        {
            SqlEventArgs e = new SqlEventArgs() { Text = strSql, CommandType = commandType, Parameters = cmdParms };
            return GetSingle(e); 
        }
        public abstract object GetSingle(SqlEventArgs e);


        /// <summary>
        /// 执行一个sql语句
        /// </summary>
        /// <param name="strSql"></param>
        /// <returns>返回受影响的行，长用于数据库的删除，添加，修改</returns>
        public int Execute(string strSql)
        {
            return Execute(strSql, CommandType.Text);
        }

        public int Execute(string strSql, CommandType commandType)
        {
            return Execute(strSql, commandType, null);
        }

        public  int Execute(string strSql, CommandType commandType, DbParameter[] cmdParms)
        {
             SqlEventArgs e = new SqlEventArgs() { Text = strSql, CommandType = commandType, Parameters = cmdParms };
             return Execute(e); 
        }
        public abstract int Execute(SqlEventArgs e);
        /// <summary>
        /// 执行一组sql语句 用事务来一次执行多条sql语句
        /// </summary>
        /// <param name="StrSqls"></param>
        /// <returns>执行成功返回真</returns>
        public abstract bool Execute(List<SqlEventArgs> e);
        
 
        /// <summary>
        /// 获取数据，返回一个DbDataReader （调用后需要关闭数据库连接）
        /// </summary>
        /// <param name="strSql"></param>
        /// <returns></returns>
        public DbDataReader GetDataReader(string strSql)
        {
            return GetDataReader(strSql, CommandType.Text);
        }
        public DbDataReader GetDataReader(string strSql,CommandType commandType)
        {
            return GetDataReader(strSql, commandType,null);
        }
        public DbDataReader GetDataReader(string strSql, CommandType commandType, DbParameter[] cmdParms)
        {
              SqlEventArgs e = new SqlEventArgs() { Text = strSql, CommandType = commandType, Parameters = cmdParms };
              return GetDataReader(e); 
        }
        public abstract DbDataReader GetDataReader(SqlEventArgs e);
         

        /// <summary>
        /// 执行一个sql语句，返回一个DataSet对象
        /// </summary>
        /// <param name="strSql"></param>
        /// <returns></returns>
        public DataSet GetDataSet(string strSql)
        {
            return GetDataSet(strSql, CommandType.Text);
        }
        public DataSet GetDataSet(string strSql, CommandType commandType)
        { 
            return GetDataSet( strSql,commandType,null);
        }
        public DataSet GetDataSet(string strSql, CommandType commandType,DbParameter[] cmdParms)
        { 
            SqlEventArgs e = new SqlEventArgs() { Text = strSql, CommandType = commandType, Parameters = cmdParms };
            return GetDataSet(e);  
        }

        public abstract DataSet GetDataSet(SqlEventArgs e);
       
         
        /// <summary>
        /// 执行一条sql语句，返回一个DataRow
        /// </summary>
        /// <param name="strSql"></param>
        /// <returns></returns> 
        public DataRow GetDataRow(string strSql)
        {
            return GetDataRow(strSql, CommandType.Text);
        }
        public DataRow GetDataRow(string strSql, CommandType commandType)
        {
            return GetDataRow(strSql, commandType, null);
        }
        public DataRow GetDataRow(string strSql, CommandType commandType, DbParameter[] cmdParms)
        {
            SqlEventArgs e = new SqlEventArgs() { Text = strSql, CommandType = commandType, Parameters = cmdParms };
            return GetDataRow(e);
        } 
        public abstract DataRow GetDataRow(SqlEventArgs e);
         

        /// <summary>
        /// 用于初始化DbCommand 
        /// </summary>
        /// <param name="cmd"></param>
        /// <param name="conn"></param>
        /// <param name="Tran"></param>
        /// <param name="cmdText"></param>
        /// <param name="cmdParas"></param> 
        protected void ParameterCommandAdd(DbCommand cmd, DbConnection conn, DbTransaction Tran, CommandType comType, string cmdText, DbParameter[] cmdParas)
        {
            try
            {
                if (conn.State != ConnectionState.Open)
                    conn.Open();
                //初始化Command
                cmd.Connection = conn;
                cmd.CommandText = cmdText;
                cmd.CommandType = comType;

                //判断是否为事务处理
                if (Tran != null)
                    cmd.Transaction = Tran;

                if (cmdParas != null)
                {
                    cmd.Parameters.Clear();
                    foreach (DbParameter para in cmdParas)
                    {
                        cmd.Parameters.Add(para);
                    }
                }
            }catch(Exception e)
            {}
             
        }

#endregion
    }

    public class SqlEventArgs : EventArgs
    {
        private string _text = string.Empty;
        private CommandType _commandType = CommandType.Text;
        private DbParameter[] _parameters = null;

        public string Text {
            get { return _text; }
            set { _text = value; }
        }
        public CommandType CommandType {
            get { return _commandType; }
            set { _commandType = value; }
        }
        public DbParameter[] Parameters {
            get { return _parameters; }
            set { _parameters = value; }
        }
    }
   
}