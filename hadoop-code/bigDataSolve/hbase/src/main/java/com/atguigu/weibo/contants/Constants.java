package com.atguigu.weibo.contants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 10:54 2021/4/15
 */

public class Constants {
    //HBase的配置信息
    public static final Configuration CONFIGURATION = HBaseConfiguration.create();
    //命名空间
    public static final String NAMESPACE = "weibo";

    //表:微博内容表
    public static final String CONTENT_TABLE = "weibo:content";
    public static final String CONTENT_TABLE_CF = "info";
    public static final int CONTENT_TABLE_VERSIONS = 1;

    //表：用户关系表
    public static final String RELATION_TABLE = "weibo:relation";
    public static final String RELATION_TABLE_CF1 = "attends";
    public static final String RELATION_TABLE_CF2 = "fans";
    public static final int RELATION_TABLE_VERSIONS = 1;

    //表：收件箱表
    public static final String INBOX_TABLE = "weibo:inbox";
    public static final String INBOX_TABLE_CF = "info";
    public static final int INBOX_TABLE_VERSIONS = 2;
}
