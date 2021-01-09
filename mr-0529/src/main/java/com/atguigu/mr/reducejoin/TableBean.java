package com.atguigu.mr.reducejoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 21:29 2021/1/3
 */


public class TableBean implements WritableComparable<TableBean> {

    private String id;
    private String pid;
    private int amount;
    private String pName;


    @Override
    public int compareTo(TableBean o) {
        int res = this.pid.compareTo(o.pid);
        if(res == 0){
            return o.pName.compareTo(this.pName);  // 按照pName降序，则pd中的记录在首行
        }else{
            return res;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(pName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readUTF();
        pid = in.readUTF();
        amount = in.readInt();
        pName = in.readUTF();
    }

    @Override
    public String toString() {
        return id + "\t" + pName + "\t" + amount;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getpName() {
        return pName;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

}
