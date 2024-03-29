package com.lh.common;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 16:51 2022/8/21
 */


public class GmallConfig {

    //Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL210325_REALTIME";

    //Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    //Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:node01,node02,node03:2181";

    //ClickHouse_Url
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://node01:8123/default";

    //ClickHouse_Driver
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

}
