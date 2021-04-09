package com.atguigu.mr.order.backup;

import com.atguigu.mr.order.OrderBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 16:03 2021/1/2
 */


public class OGroupingComparator extends WritableComparator {

    protected OGroupingComparator(){
        super(OBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OBean o1 = (OBean) a;
        OBean o2 = (OBean) b;
//        System.out.println(o1 + " /////////////////////   " + o2);
        return o1.getOrderId().compareTo(o2.getOrderId());
    }

}
