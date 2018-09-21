using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkData
{
    /// <summary>
    /// 2.0版本，
    /// 在对类反射的时候，区分出了类中的类属性和集合属性
    /// </summary>
    public static class BulkCopy
    {
        /// <summary>
        /// 批量插入数据
        /// </summary>
        /// <typeparam name="TModel"></typeparam>
        /// <param name="modelList">数据集合</param>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="tableName">表名</param>
        public static void BulkInert<TModel>(IList<TModel> modelList, string connectionString, string tableName)
        {
            try
            {
                using (SqlConnection sqlConnect = new SqlConnection(connectionString))
                {
                    DataTable dt = ToSqlBulkCopyDataTable(modelList, sqlConnect, tableName);
                    SqlBulkCopy sqlBulk = null;
                    sqlBulk = new SqlBulkCopy(sqlConnect);
                    using (sqlBulk)
                    {
                        sqlBulk.DestinationTableName = tableName;
                        if (sqlConnect.State != ConnectionState.Open)
                        {
                            sqlConnect.Open();
                        }
                        sqlBulk.WriteToServer(dt);
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// 生成DataTable
        /// </summary>
        /// <typeparam name="TModel"></typeparam>
        /// <param name="modelList">数据集合</param>
        /// <param name="conn">SqlConnection对象</param>
        /// <param name="tableName">表名</param>
        /// <returns></returns>
        private static DataTable ToSqlBulkCopyDataTable<TModel>(IList<TModel> modelList, SqlConnection conn, string tableName)
        {
            DataTable dt = new DataTable();
            #region 获取所有表字段
            string sql = string.Format("select top 0 * from {0}", tableName);
            if (conn.State != ConnectionState.Open)
            {
                conn.Open();
            }
            SqlCommand command = new SqlCommand();
            command.CommandText = sql;
            command.CommandType = CommandType.Text;
            command.Connection = conn;
            SqlDataReader reader = command.ExecuteReader();
            try
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var re_name = reader.GetName(i);
                    var re_type = reader.GetFieldType(i);
                    DataColumn column = new DataColumn(re_name, re_type);
                    dt.Columns.Add(column);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                }
            }
            #endregion
            //获取实体
            Type mType = typeof(TModel);
            var mType_Properts = mType.GetProperties();

            foreach (var model in modelList)
            {
                DataRow dr = dt.NewRow();
                foreach (var proper in mType_Properts)
                {
                    string fullName = proper.PropertyType.FullName;
                    bool isValueType = proper.PropertyType.IsValueType;
                    bool isClass = proper.PropertyType.IsClass;
                    bool isEnum = proper.PropertyType.IsEnum;
                    if ((isValueType || isEnum) && !isClass)
                    {
                        object value = proper.GetValue(model);
                        if (proper.PropertyType.IsEnum)
                        {
                            if (value != null)
                            {
                                value = (int)value;
                            }
                        }
                        dr[proper.Name] = value ?? DBNull.Value;
                    }
                    else if (fullName == "System.String")
                    {
                        object value = proper.GetValue(model);
                        dr[proper.Name] = value ?? DBNull.Value;
                    }
                }
                dt.Rows.Add(dr);
            }
            return dt;
        }

        /// <summary>
        /// 批量修改数据
        /// </summary>
        /// <param name="modelList"></param>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="tableName">表名</param>
        /// <param name="primaryKey">主键</param>
        /// <returns></returns>
        public static int BulkUpdate<TModel>(IList<TModel> modelList, string connectionString, string tableName, string primaryKey)
        {
            try
            {
                Debug.WriteLine("进入BulkCopy");
                //临时表名使用日期加随机数
                Random ran = new Random();
                int ranNumber = ran.Next(1, 10000);
                string dateStr = DateTime.Now.ToString("yyyyMMddHHmmss");
                string tempName = "#" + tableName + dateStr + ranNumber;
                var model = typeof(TModel);
                var propers = model.GetProperties();
                StringBuilder updateStrBuild = new StringBuilder();

                foreach (var item in propers)
                {
                    string fullName = item.PropertyType.FullName;
                    bool isValueType = item.PropertyType.IsValueType;
                    bool isClass = item.PropertyType.IsClass;
                    bool isEnum = item.PropertyType.IsEnum;
                    if ((isValueType || isEnum) && !isClass)
                    {
                        updateStrBuild.Append(" t2." + item.Name + " = t1." + item.Name + ",");
                    }
                    else if (fullName == "System.String")
                    {
                        updateStrBuild.Append(" t2." + item.Name + " = t1." + item.Name + ",");
                    }
                }

                string updaSql = updateStrBuild.ToString();
                updaSql = updaSql.TrimEnd(',');
                Debug.WriteLine("修改语句" + updaSql);
                string updateSql = string.Format("update t2 SET {2}  FROM  {0} AS t1,{1} AS t2 WHERE t1.{3} = t2.{3}", tempName, tableName, updaSql, primaryKey);
                Debug.WriteLine(updateSql);
                StringBuilder strB = new StringBuilder();
                foreach (var item in propers)
                {
                    string fullName = item.PropertyType.FullName;
                    bool isValueType = item.PropertyType.IsValueType;
                    bool isClass = item.PropertyType.IsClass;
                    bool isEnum = item.PropertyType.IsEnum;
                    if ((isValueType || isEnum) && !isClass)
                    {
                        strB.Append(" " + item.Name + ", ");
                    }
                    else if (fullName == "System.String")
                    {
                        strB.Append(" " + item.Name + ", ");
                    }
                }
                strB.Append(" " + primaryKey + " as Ids ");
                string strSql = strB.ToString();
                using (SqlConnection conn = new SqlConnection(connectionString))
                {
                    string sql = "SELECT top 0 " + strSql + " into " + tempName + " from  " + tableName;
                    if (conn.State != ConnectionState.Open)
                    {
                        conn.Open();
                    }
                    SqlCommand command = new SqlCommand();
                    command.CommandText = sql;
                    command.CommandType = CommandType.Text;
                    command.Connection = conn;
                    command.ExecuteNonQuery();
                    DataTable dt = ToSqlBulkCopyDataTable2(modelList, conn, tableName, strSql, primaryKey);
                    SqlBulkCopy sqlBulk = null;
                    sqlBulk = new SqlBulkCopy(conn);
                    using (sqlBulk)
                    {
                        sqlBulk.DestinationTableName = tempName;
                        if (conn.State != ConnectionState.Open)
                        {
                            conn.Open();
                        }
                        sqlBulk.WriteToServer(dt);
                    }
                    command.CommandText = updateSql;
                    int count = command.ExecuteNonQuery();
                    return count;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// 批量修改准备DataTable
        /// </summary>
        /// <typeparam name="TModel"></typeparam>
        /// <param name="modelList"></param>
        /// <param name="conn"></param>
        /// <param name="tableName"></param>
        /// <param name="strSql"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        private static DataTable ToSqlBulkCopyDataTable2<TModel>(IList<TModel> modelList, SqlConnection conn, string tableName, string strSql, string key)
        {
            Debug.WriteLine("进入ToSqlBulkCopyDataTable2");
            DataTable dt = new DataTable();
            //获取实体
            Type mType = typeof(TModel);
            var mType_Properts = mType.GetProperties();

            #region
            string sql = string.Format("select top 1 " + strSql + " from  {0}", tableName);
            Debug.WriteLine("查询语句：" + sql);
            if (conn.State != ConnectionState.Open)
            {
                conn.Open();
            }
            SqlCommand command = new SqlCommand();
            command.CommandText = sql;
            command.CommandType = CommandType.Text;
            command.Connection = conn;
            var reader = command.ExecuteReader();
            try
            {
                while (reader.Read())
                {
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        var re_name = reader.GetName(i);
                        var re_type = reader.GetFieldType(i);
                        DataColumn column = new DataColumn(re_name, re_type);
                        dt.Columns.Add(column);
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                }
            }
            #endregion
            foreach (var model in modelList)
            {
                DataRow dr = dt.NewRow();
                foreach (var proper in mType_Properts)
                {
                    string fullName = proper.PropertyType.FullName;
                    bool isValueType = proper.PropertyType.IsValueType;
                    bool isClass = proper.PropertyType.IsClass;
                    bool isEnum = proper.PropertyType.IsEnum;
                    if ((isValueType || isEnum) && !isClass)
                    {
                        object value = proper.GetValue(model);
                        if (proper.PropertyType.IsEnum)
                        {
                            if (value != null)
                            {
                                value = (int)value;
                            }
                        }
                        dr[proper.Name] = value ?? DBNull.Value;
                        if (key.Equals(proper.Name))
                        {
                            dr["Ids"] = value;

                        }
                    }
                    else if (fullName == "System.String")
                    {
                        object value = proper.GetValue(model);
                        dr[proper.Name] = value ?? DBNull.Value;
                        if (key.Equals(proper.Name))
                        {
                            dr["Ids"] = value;

                        }
                    }

                }
                dt.Rows.Add(dr);
            }
            return dt;
        }

    }
}
