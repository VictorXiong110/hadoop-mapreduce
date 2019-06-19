package com.aura.mazh.mapreduce1903.cf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CF_Reducer2 extends Reducer<Text, Text, Text, Text>{

	/**
	 * reduce阶段的输入：
	 * key:  用户对  A-B
	 * values:  这个用户对的所有共同好友的集合：(C,D)
	 * 
	 * reduce阶段的输出：
	 * key:  B-C	用户对
	 * value:  好友集合
	 */
	Text outValue = new Text();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		StringBuilder sb = new StringBuilder();
		for(Text f:  values){
			sb.append(f.toString()).append(",");
		}
		outValue.set(sb.toString());
		
		context.write(key, outValue);
	}
}






