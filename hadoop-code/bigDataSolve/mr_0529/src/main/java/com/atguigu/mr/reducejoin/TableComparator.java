package com.atguigu.mr.reducejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 22:39 2021/1/3
 */


public class TableComparator extends WritableComparator {

    public TableComparator() {
        super(TableBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TableBean a1 = (TableBean) a;
        TableBean b1 = (TableBean) b;
        return a1.getPid().compareTo(b1.getPid());
    }
}

