package com.aura.mazh.mapreduce1903.cf;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CF_Mapper2 extends Mapper<LongWritable, Text, Text, Text>{
	
	/**
	 * mapper阶段的输入：
	 * key: 偏移量
	 * value: 一行数据   用户对  
	 * 		I-O	A
	 * 
	 * mapper阶段的输出：
	 * key: I-O
	 * value: A
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		String[] split = value.toString().split("\t");
		String keyStr = split[0];
		String valueStr = split[1];

		outKey.set(keyStr);
		outValue.set(valueStr);
		
		context.write(outKey, outValue);
	}
	Text outKey = new Text();
	Text outValue = new Text();
}
