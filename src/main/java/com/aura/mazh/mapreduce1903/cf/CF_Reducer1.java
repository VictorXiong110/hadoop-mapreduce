package com.aura.mazh.mapreduce1903.cf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CF_Reducer1 extends Reducer<Text, Text, Text, Text>{

	/**
	 * reduce阶段的输入：
	 * key: 好友	   A
	 * values:  这个好友是那些用户的好友的这一堆用户的集合  (B,C,D)
	 * 
	 * reduce阶段的输出：
	 * key:  B-C	用户对
	 * value:  A	好
	 */
	Text outKey = new Text();
	List<String> userList = new ArrayList<String>();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		userList.clear();
		/**
		 * 假设：
		 * 	userList:   C A B 	===>  ABC
		 * 连接结果：
		 * 	 C-A
		 *   C-B
		 *   A-B	F
		 *   
		 * 假设：
		 * 	 userList:   B A D	====>  ABD
		 * 连接结果：
		 * 	 B-A	E
		 *   B-D
		 *   A-D
		 */
		
		for(Text t: values){
			userList.add(t.toString());
		}
		
		Collections.sort(userList);
		
		int size = userList.size();
		// 连接操作
		for(int i=0; i<size-1; i++){
			for(int j=i+1; j<size; j++){
				String keyStr = userList.get(i) + "-" + userList.get(j);
				
				outKey.set(keyStr);
				context.write(outKey, key);
			}
		}
	}
}






