package com.aura.mazh.mapreduce1903.flow;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CityPartitioner extends Partitioner<FlowBean, NullWritable>{

	/**
	 * key
	 * value
	 * numPartitions： 分区个数  reduceTask的个数
	 * 
	 * 按照地域 ，相同地域的数据，就放置在同一个文件中
	 * 
	 * 暴力假设：
	 *  134    北京	0
	 *  135  上海	1
	 *  136 天津	2
	 *  ....
	 */
	@Override
	public int getPartition(FlowBean key, NullWritable value, int numPartitions) {

		String phonePrefix = key.getPhone().substring(0, 3);
		Integer number = Integer.valueOf(phonePrefix);
		if(number == 134){
			return 0;
		}else if(number == 135){
			return 1;
		}else if(number == 136){
			return 3;
		}else if(number == 137){
			return 4;
		}else{
			return 5;
		}
	}

	
}
