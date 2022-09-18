package com.lh.bean;

import lombok.Data;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 10:09 2022/8/17
 */


@Data
public class TableProcess {
    public static final String SINK_TABLE_HBASE = "hbase";
    public static final String SINK_TABLE_KAFKA = "kafka";
    public static final String SINK_TABLE_CK = "cickhouse";

    String source_table;

    String operate_type;

    String sink_type;

    String sink_table;

    String sink_columns;

    String sink_pk;

    String sink_extend;

}
