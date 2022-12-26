package com.lh.app.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lh.bean.TableProcess;
import com.lh.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 10:33 2022/8/17
 */



public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    OutputTag<JSONObject> hBaseTag;
    Connection connection;


    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor, OutputTag<JSONObject> hBaseTag) {
        this.mapStateDescriptor = mapStateDescriptor;
        this.hBaseTag = hBaseTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        // value {db:, tn:, after:{}, before:{}, type:}
        JSONObject data = JSON.parseObject(value);
        TableProcess after = JSON.parseObject(data.getString("after"), TableProcess.class);
        if(TableProcess.SINK_TABLE_HBASE.equals(after.getSink_type())){
            checkTable(after);
        }
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = after.getSource_table() + "-" + after.getOperate_type();
        System.out.println("需要从TableProcess广播的key >>>>>>>>>>>>>" + key);
        broadcastState.put(key, after);
    }

    private void checkTable(TableProcess after) {
        PreparedStatement preparedStatement = null;
        try {
            String schema = GmallConfig.HBASE_SCHEMA;
            String sinkTable =  after.getSink_table();
            String sinkPk = after.getSink_pk();
            String sinkExtend = after.getSink_extend();

            String[] sinkColumns = after.getSink_columns().split(",");

            StringBuilder sbu = new StringBuilder();
            sbu.append("create table if not exists "  + schema + "." + sinkTable + "(");
            if(sinkPk==null){
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }
            for (int i = 0; i < sinkColumns.length; i++) {
                String col = sinkColumns[i];
                if(col.equals(sinkPk)){
                    sbu.append(col + " varchar primary key ");
                }else{
                    sbu.append(col + " varchar ");
                }
                if(i<sinkColumns.length-1){
                    sbu.append(",");
                }
            }
            sbu.append(")").append(sinkExtend);
            String execSql = sbu.toString();
            System.out.println("Phoenix建表语句>>>>>>>>>>>>>>>>>>>>>>>" + execSql);


            preparedStatement = connection.prepareStatement(execSql);
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


    }


    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

        JSONObject after = value.getJSONObject("after");
        System.out.println("value" + value);
        String key = value.getString("table") + "-" + value.getString("type");
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        TableProcess tableProcess = broadcastState.get(key);
        Iterator<Map.Entry<String, TableProcess>> iterator = broadcastState.immutableEntries().iterator();
        while (iterator.hasNext()){
            System.out.println("processElement>>>>>>>>>>>>>>" + iterator.next());
        }
        if(tableProcess!=null){
            filterCols(after, tableProcess.getSink_columns());
            value.put("sinkTable", tableProcess.getSink_table());
            if(TableProcess.SINK_TABLE_HBASE.equals(tableProcess.getSink_type())){
                ctx.output(hBaseTag, value);
            }else if(TableProcess.SINK_TABLE_KAFKA.equals(tableProcess.getSink_type())){

                out.collect(value);
            }else{
                System.out.println(tableProcess.getSink_type() + " 不正确！");
            }
        }else{
            System.out.println("key " + key + " 不存在！");
        }

    }

    private void filterCols(JSONObject mainData, String sinkColumns){
        String[] cols = sinkColumns.split(",");
        List<String> list = Arrays.asList(cols);
        mainData.entrySet().removeIf(ele -> !list.contains(ele.getKey()));

    }


}

//
//public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
//
//    private OutputTag<JSONObject> objectOutputTag;
//    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
//    private Connection connection;
//
//    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor,
//                                OutputTag<JSONObject> objectOutputTag ) {
//        this.mapStateDescriptor = mapStateDescriptor;
//        this.objectOutputTag = objectOutputTag;
//
//    }
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        Class.forName(GmallConfig.PHOENIX_DRIVER);
//        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
//    }
//
//    //value:{"db":"","tn":"","before":{},"after":{},"type":""}
//    @Override
//    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
//
//        //1.获取并解析数据
//        JSONObject jsonObject = JSON.parseObject(value);
//        String data = jsonObject.getString("after");
//        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);
//        System.out.println("broad>>>>>>>>>>>>>>" + value);
//        //2.建表
//        if (TableProcess.SINK_TABLE_HBASE.equals(tableProcess.getSink_type())) {
//            checkTable(tableProcess.getSink_table(),
//                    tableProcess.getSink_columns(),
//                    tableProcess.getSink_pk(),
//                    tableProcess.getSink_extend());
//        }
//
//        //3.写入状态,广播出去
//        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
//        String key = tableProcess.getSource_table() + "-" + tableProcess.getOperate_type();
//        broadcastState.put(key, tableProcess);
//    }
//
//    //建表语句 : create table if not exists db.tn(id varchar primary key,tm_name varchar) xxx;
//    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
//        System.out.println("开始建表");
//        PreparedStatement preparedStatement = null;
//
//        try {
//            if (sinkPk == null) {
//                sinkPk = "id";
//            }
//            if (sinkExtend == null) {
//                sinkExtend = "";
//            }
//
//            StringBuffer createTableSQL = new StringBuffer("create table if not exists ")
//                    .append(GmallConfig.HBASE_SCHEMA)
//                    .append(".")
//                    .append(sinkTable)
//                    .append("(");
//
//            String[] fields = sinkColumns.split(",");
//
//            for (int i = 0; i < fields.length; i++) {
//                String field = fields[i];
//
//                //判断是否为主键
//                if (sinkPk.equals(field)) {
//                    createTableSQL.append(field).append(" varchar primary key ");
//                } else {
//                    createTableSQL.append(field).append(" varchar ");
//                }
//
//                //判断是否为最后一个字段,如果不是,则添加","
//                if (i < fields.length - 1) {
//                    createTableSQL.append(",");
//                }
//            }
//
//            createTableSQL.append(")").append(sinkExtend);
//
//            //打印建表语句
//            System.out.println(createTableSQL);
//
//            //预编译SQL
//            preparedStatement = connection.prepareStatement(createTableSQL.toString()); // 这里没做连接池，所以只有一个，就不在finally中关闭了
//
//            //执行
//            preparedStatement.execute();
//        } catch (SQLException e) {
//            throw new RuntimeException("Phoenix表" + sinkTable + "建表失败！");
//        } finally {
//            if (preparedStatement != null) {
//                try {
//                    preparedStatement.close();
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//    }
//
//    //value:{"db":"","tn":"","before":{},"after":{},"type":""}
//    @Override
//    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
//
//        //1.获取状态数据
//
////        System.out.println("主流>>>>>>>>>>>>>>>>>>>>" + value);
//        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
//        String key = value.getString("tableName") + "-" + value.getString("type");
//
//        TableProcess tableProcess = broadcastState.get(key);
//
//        if (tableProcess != null) {
//
//            //2.过滤字段
//            JSONObject data = value.getJSONObject("after");
//            filterColumn(data, tableProcess.getSink_columns());
//
//            //3.分流
//            //将输出表/主题信息写入Value
//            value.put("sinkTable", tableProcess.getSink_table());
//            String sinkType = tableProcess.getSink_type();
//            if (TableProcess.SINK_TABLE_KAFKA.equals(sinkType)) {
//                //Kafka数据,写入主流
//                out.collect(value);
//            } else if (TableProcess.SINK_TABLE_HBASE.equals(sinkType)) {
//                //HBase数据,写入侧输出流
//                ctx.output(objectOutputTag, value);
//            }else{
//                System.out.println("sinkType " + sinkType + " 不正确");
//            }
//
//        } else {
//            System.out.println("该组合Key：" + key + "不存在！");
//        }
//    }
//
//    /**
//     * @param data        {"id":"11","tm_name":"atguigu","logo_url":"aaa"}
//     * @param sinkColumns id,tm_name
//     *                    {"id":"11","tm_name":"atguigu"}
//     */
//    private void filterColumn(JSONObject data, String sinkColumns) {
//
//        String[] fields = sinkColumns.split(",");
//        List<String> columns = Arrays.asList(fields);
//
////        Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
////        while (iterator.hasNext()) {
////            Map.Entry<String, Object> next = iterator.next();
////            if (!columns.contains(next.getKey())) {
////                iterator.remove();
////            }
////        }
//
//        data.entrySet().removeIf(next -> !columns.contains(next.getKey()));
//
//    }
//}