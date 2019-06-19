package com.aura.mazh.mapreduce1903.cf;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CF_Mapper1 extends Mapper<LongWritable, Text, Text, Text>{
	
	/**
	 * mapper阶段的输入：
	 * key: 偏移量
	 * value: 一行数据   B:A,C,E,K
	 * 
	 * mapper阶段的输出：
	 * key: 好友
	 * value: 用户
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		String[] split = value.toString().split(":");
		String user = split[0];
		
		String[] friends = split[1].split(",");
		
		for(String friend:  friends){
			
			outKey.set(friend);
			outValue.set(user);
			context.write(outKey, outValue);
		}

	}
	Text outKey = new Text();
	Text outValue = new Text();
}
