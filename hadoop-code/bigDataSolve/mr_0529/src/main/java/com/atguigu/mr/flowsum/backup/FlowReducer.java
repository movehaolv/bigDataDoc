package com.atguigu.mr.flowsum.backup;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 19:06 2020/12/13
 */


public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    FlowBean flowBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long upFlow = 0;
        long downFlow = 0;
        for(FlowBean value:values){
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
        }
        flowBean.set(upFlow, downFlow);
        context.write(key, flowBean);


    }
}
