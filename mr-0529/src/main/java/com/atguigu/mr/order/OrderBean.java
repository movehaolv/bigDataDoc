package com.atguigu.mr.order;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class OrderBean implements WritableComparable<OrderBean>{

	private int orderId;		// 订单id
	private double price;		// 价格
	
	public OrderBean() {
		super();
	}
	
	public OrderBean(int order_id, double price) {
		super();
		this.orderId = order_id;
		this.price = price;
	}



	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(orderId);
		out.writeDouble(price);
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		orderId = in.readInt();
		price = in.readDouble();
	}

	@Override
	public int compareTo(OrderBean bean) {
		
		// 先按照定id升序排序，如果相同 按照价格降序排序
		int result;
		System.out.println();
		if (orderId > bean.getOrderId()) {
			result = 1;
		}else if (orderId < bean.getOrderId()) {
			result = -1;
		}else {
			
			if (price > bean.getPrice()) {
				result = -1;
			}else if (price < bean.getPrice()) {
				result = 1;
			}else {
				result = 0;
			}
		}
		
		return result;
	}

	public int getOrderId() {
		return orderId;
	}

	public void setOrderId(int orderId) {
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
		return orderId + "\t" + price;
	}
}
