package com.lh.app.dws;

import com.lh.app.function.KeySplitFunc;
import com.lh.bean.KeywordStats;
import com.lh.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 13:17 2022/11/16
 */

//KeywordStats(keyword=口红, ct=2, source=search, stt=2022-11-10 17:51:50, edt=2022-11-10 17:52:00, ts=1668576095000)

public class KeywordStatsApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
        env.setParallelism(1);
        String sourceTopic = "dwd_page_log";
        String group = "keyWord";
        tableEnv.executeSql("create table if not exists pageLog (" +
                "`common` map<String, String>, " +
                "`page` map<String, String>," +
                "`ts` BIGINT," +
                "`rt` as to_timestamp(from_unixtime(ts/1000))," +
                "watermark for rt as rt - interval '10' second) with (" +
                 MyKafkaUtil.getKafkaDDL(sourceTopic,group) + ")");
        Table keyWordTable = tableEnv.sqlQuery("select page['item'] as search_word,rt from pageLog " +
                "where page['last_page_id']='search' and page['item'] is not null ");

        tableEnv.createTemporaryFunction("splitKeyWords", KeySplitFunc.class);
        Table sqlQuery = tableEnv.sqlQuery("select " +
                "'search' as source," +
                "date_format(tumble_start(rt, interval '10' second), 'yyyy-MM-dd HH:mm:ss') stt," +
                "date_format(tumble_end(rt, interval '10' second), 'yyyy-MM-dd HH:mm:ss') edt," +
                "word as keyword, " +
                "count(*) as ct," +
                "UNIX_TIMESTAMP() * 1000 as ts " +
                " from " + keyWordTable + ", LATERAL TABLE(splitKeyWords(search_word)) " +
                "group by tumble(rt, interval '10' second),word");

        sqlQuery.printSchema();
        tableEnv.toAppendStream(sqlQuery, KeywordStats.class).print();
        env.execute();

    }
}
