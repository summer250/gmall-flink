package com.gmall.flink.realtime.utils;

import com.gmall.flink.realtime.bean.TableProcess;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Desc: 从MySQL数据中查询数据的工具类
 * 完成ORM,对象关系映射
 * O:Object对象     Java中对象
 * R:Relation      关系型数据库
 * M:Mapping       将Java中的对象和关系数据库的表中的记录建立起映射关系
 **/

public class MysqlUtil {
    /**
     * @param sql               执行的查询语句
     * @param clz               返回的数据类型
     * @param underScoreToCamel 是否将下划线转换为驼峰命名法
     * @param <T>
     * @return
     */
    public static <T> List<T> queryList(String sql, Class<T> clz, Boolean underScoreToCamel) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //注册驱动
            Class.forName("com.mysql.jdbc.Driver");
            //建立连接
            conn = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/gmall-flink_realtime?characterEncoding=utf-8&useSSL=false",
                    "root", "12345678");
            //创建数据库操作对象
            ps = conn.prepareStatement(sql);
            //执行SQL语句
            //100   zs  20
            //200   ls  30
            rs = ps.executeQuery();
            //处理结果集
            //查询结果元数据信息
            //id    student_name    age
            ResultSetMetaData metaData = rs.getMetaData();
            List<T> resultList = new ArrayList<T>();
            //判断结果集中是否存在数据,如果有,那么进行一次循环
            while (rs.next()) {
                //创建一个对象,用于封装查询出来一条结果集中的数据
                T obj = clz.newInstance();
                //对查询的所有列进行遍历,获取每一列的名称
                for (int i = 1; i < metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    String propertyName = "";
                    if (underScoreToCamel) {
                        //如果指定的下划线转换为驼峰命名法的值为true  通过guava工具类将表中的列转换为类属性的驼峰命名法的形式
                        propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    //调用Apache的common-bean工具类,给obj属性赋值
                    BeanUtils.setProperty(obj, propertyName, rs.getObject(i));
                }
                //将当前结果中的一行数据封装的obj对象放到list集合中
                resultList.add(obj);
            }
            return resultList;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从MySQL查询数据失败");
        } finally {
            //释放资源
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        List<TableProcess> list = queryList("select * from table_process", TableProcess.class, true);
        for (TableProcess tableProcess : list) {
            System.out.println(tableProcess);
        }
    }
}
