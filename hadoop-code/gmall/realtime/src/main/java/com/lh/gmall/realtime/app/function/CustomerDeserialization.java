package com.lh.gmall.realtime.app.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import jdk.nashorn.internal.ir.CallNode;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;


/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 13:40 2022/8/10
 */


public class CustomerDeserialization implements DebeziumDeserializationSchema<String> {


    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String schema = fields[1];
        String table = fields[2];
        Struct value = (Struct)sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if(before!=null){
            Schema beforeSchema = before.schema();
            List<Field> beforeFields = beforeSchema.fields();
            for(Field field:beforeFields){
                String name = field.name();
                Object beforeValue = before.get(name);
                beforeJson.put(name, beforeValue);
            }
        }

        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if(after!=null){
            Schema afterSchema = after.schema();
            List<Field> afterFields = afterSchema.fields();
            for(Field field:afterFields){
                String name = field.name();
                Object afterValue = after.get(name);
                afterJson.put(name, afterValue);
            }
        }

        JSONObject ret = new JSONObject();
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if("create".equals(type)){
            type = "insert";
        }
        ret.put("database", schema);
        ret.put("table", table);
        ret.put("before", beforeJson);
        ret.put("after", afterJson);
        ret.put("type", type);

        collector.collect(ret.toJSONString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
