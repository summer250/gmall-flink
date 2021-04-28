package com.gmall.flink.realtime.common;

/**
 * Desc: 项目配置的常量表
 **/
public class GmallConfig {
    //hbase的命名空间
    public static final String HBASE_SCHEMA = "GMALL_FLINK_REALTIME";

    //Phoenix连接的服务器地址
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop:2181?characterEncoding=utf-8";

}
