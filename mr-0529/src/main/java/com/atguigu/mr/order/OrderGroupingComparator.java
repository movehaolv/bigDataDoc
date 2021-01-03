package com.atguigu.mr.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator{

	protected OrderGroupingComparator(){
		super(OrderBean.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// 要求只要id相同，就认为是相同的key

		OrderBean aBean = (OrderBean) a;
		OrderBean bBean = (OrderBean) b;
		System.out.println(aBean + " /////////////////////   " + bBean);
		int result;
		if(aBean.getOrderId() == bBean.getOrderId()){
			System.out.println(aBean + " /////////////////////++++++++++++++++++++   " + bBean);
			result = 0;
		}else {
			return 1;
		}

//		if (aBean.getOrderId() > bBean.getOrderId()) {
//			result = 1;
//		}else if(aBean.getOrderId() < bBean.getOrderId()){
//			result = -1;
//		}else {
//			result = 0;
//		}

		return result;
	}
	
}
