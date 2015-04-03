using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ZW.DbBasic
{

    public enum DbType { Mysql,SqlServer}
    /// <summary>
    /// 创建数数据库访问类   
    /// </summary>
    public class DbFactory
    { 
        /// <summary>
        /// 工厂
        /// </summary>
        /// <returns></returns>
        public static DBHelper DbCreate(DbType DbType, string ConnectionString)
        { 
            switch (DbType)
            {
                case global::ZW.DbBasic.DbType.Mysql:
                     return new MysqlHelper(ConnectionString);
                case  global::ZW.DbBasic.DbType.SqlServer:
                    return new SqlServerHelper(ConnectionString); 
                default:
                    return null;
            }
        }  
    }



}
