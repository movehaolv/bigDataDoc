package com.atguigu.mr.order;

import java.io.IOException;
import java.util.Iterator;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

	@Override
	protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context)		throws IOException, InterruptedException {
		System.out.println("++++++++++++++++++++++++" + Iterables.size(values));
		context.write(key, NullWritable.get());
	}
}