package com.atguigu.mr.order.backup;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 16:03 2021/1/2
 */
public class OBean implements WritableComparable<OBean>{

    private String orderId;		// 订单id
    private double price;		// 价格
    private String id;

    public OBean() {
        super();
    }

    public OBean(String orderId, double price) {
        super();
        this.orderId = orderId;
        this.price = price;
    }



    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(orderId);
        out.writeDouble(price);
        out.writeUTF(id);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        orderId = in.readUTF();
        price = in.readDouble();
        id = in.readUTF();
    }

        @Override
    public int compareTo(OBean o) {
            System.out.println(this + " ------------------------------- " + o);
            if(this.orderId.equals(o.orderId)){
            if(this.price == o.price){
                return 0;
            }else if(this.price > o.price){
                return -1;
            }else {
                return 1;
            }
        }else{
            int res =  this.orderId.compareTo(o.orderId);
            return res;
        }

    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return orderId + "\t" + price + "\t" + id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}

