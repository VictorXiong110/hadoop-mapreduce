package com.aura.mazh.mapreduce1903.join.map;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *  思路的复习
 *  1、把大文件当做是单独的输入文件，
 *  2、大文件有多少个数据块，就会启动多少个task
 *  3、小表的数据缓存在一个公开的数据源中，所有的mapTask需要就自己去读
 *  4、由于大表的一个数据块和小表的所有数据完成连接是在mapper阶段完成
 *  	所以称之为mapjoin 
 *	
 *	5、mapper阶段处理大表的数据。当然也要读取小表的所有数据，完成连接
 *  6、mapjoin不需要shuffle和reducre阶段。mapper阶段就完成了join处理
 */
public class MapJoinMR {

	public static void main(String[] args) throws Exception {
		
		// 1 参数
		String inputPath = args[0];
		String outputPath = args[1];

		// 2 job对象
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 3 设置jar包路径
		job.setJarByClass(MapJoinMR.class);
		
		
		// 小表文件的全局缓存做好了。
		URI uri = new URI(args[2]);
		job.addCacheFile(uri);
		

		// 4 设置各种逻辑处理组件
		job.setMapperClass(MapJoinMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		
		// 5 设置输入
		FileInputFormat.addInputPath(job, new Path(inputPath));

		// 6 设置输出
		Path output = new Path(outputPath);
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			if (fs.exists(output)) {
				fs.delete(output, true);
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		FileOutputFormat.setOutputPath(job, output);

		// 7 任务提交
		boolean isDone = job.waitForCompletion(true);
		System.exit(isDone ? 0 : -1);
		
	}
	
	static class MapJoinMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		// 存储小表所有数据
		// idAgeMap当中的所有数据的初始化必须在所有的map方法执行之前完成
		// value:  存储的是这个key所对应的所有的value
		Map<String, List<String>> idAgeMap = new HashMap<>();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			
			// 从公共数据源中，读取小表文件的所有数据， 存储到idAgeMap
			// 默认已经完成
			Path[] localFiles = context.getLocalCacheFiles();
			Path cacheFile = localFiles[0];
			String filePath = cacheFile.toUri().getPath();
			
			// 读取文件存储到Map中
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new FileInputStream(new File(filePath))));
			
			String line = null;
			while((line = br.readLine()) != null){
				
				// line : c:4:2018-01-01
				//        c:3:2018-01-01
				String[] split = line.split(":");
				String id = split[0];
				String age = split[1];
				
				// 判断这个key是否已经存在了idAgeMap中
				if(idAgeMap.containsKey(id)){
					// 如果存在，就把现在解析到的这个age现在到原来的哪个value集合list中
					List<String> list = idAgeMap.get(id);
					list.add(age);
					idAgeMap.put(id, list);
				}else{
					List<String> list = new ArrayList<String>();
					list.add(age);
					idAgeMap.put(id, list);
				}
			}
			
			br.close();
		}
		
		/**
		 * value: 
		 * 		大表中的一张数据
		 * 
		 * 		格式：
		 * 		b,xuzheng
				c,wangbaoqiang
				c,huanglei
		 */
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			String[] split = value.toString().split(",");
			String id = split[0];
			String name = split[1];
			List<String> ages = idAgeMap.get(id);
			if(ages != null){
				keyOut.set(id);
				for(String age: ages){
					valueOut.set(name + "\t" + age);
					context.write(keyOut, valueOut);
				}
			}
		}
		
		Text keyOut  = new Text();
		Text valueOut = new Text();
		
	}
}






