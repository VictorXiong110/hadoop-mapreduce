package com.aura.mazh.mapreduce1903.join.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 	filea表：
 *  a,huangbo
 *  b,xuzheng
 *  
 *  字段意义：id,name
 *  
 *  fileb表：
 *  a,1,2018-01-01
 *  b,2,2018-01-01
 *  c,3,2018-01-01
 *  
 *  字段意义：
 *  id,age,date
 *  
*  需求：  join内连接
*  select filea.id, filea.name, fileb.age from 
*   filea join fileb on filea.id = fileb.id;
 */
public class ReduceJoinMR {
	
	public static void main(String[] args) throws Exception {
		
		// 1 参数
		String inputPath = "D:\\testdata\\join\\input\\";
		String outputPath = "D:\\testdata\\join\\join_reduce\\";

		// 2 job对象
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 3 设置jar包路径
		job.setJarByClass(ReduceJoinMR.class);

		// 4 设置各种逻辑处理组件
		job.setMapperClass(RJ_Mapper.class);
		job.setReducerClass(RJ_Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
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

	/**
	 * key:  id
	 * value: 	name, age
	 * 
	 * setup(context);
    try {
      while (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
      }
    } finally {
      cleanup(context);
    }
    
    setup 就是给你在  所有map方法执行之前， 进行一些初始化的
    cleanup 就是你在 所有map方法执行之后， 进行一些资源回收的
	 */
	static class RJ_Mapper extends Mapper<LongWritable, Text, Text, Text>{
		
		String name = null;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			
			// FileInputFormat的子类：TextInputputFormat
			InputSplit inputSplit = context.getInputSplit();
			FileSplit fileSplit = (FileSplit)inputSplit;
			name = fileSplit.getPath().getName();
			
		}
		
		Text keyOut = new Text();
		Text valueOut = new Text();
		
		/**
		 * 核心问题： 这个map方法的参数 value 到底是来自于哪个文件 不知道
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			if(name.equals("a.txt")){
				// value = a,huangbo
				String[] split = value.toString().split(",");
				String name = split[1];
				String id = split[0];
				keyOut.set(id);
				valueOut.set("a--" + name);
				context.write(keyOut, valueOut);
			}else{
				// b.txt      c:4:2018-01-01
				String[] split = value.toString().split(":");
				String id = split[0];
				String age = split[1];
				keyOut.set(id);
				valueOut.set("b--" + age);
				context.write(keyOut, valueOut);
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

				// nothing 
		}
	}
	
	
	static class RJ_Reducer extends Reducer<Text, Text, Text, Text>{
		
		Text valueOut = new Text();
		
		// a 表中的参数key的所有的value
		List<String> nameList = new ArrayList<String>();
		// b 表中的参数key的所有的value
		List<String> ageList = new ArrayList<String>();
		
		/**
		 * key :  id
		 * value:
		 * 		来自于a表的a--name
		 *      来自于b表的b--age 
		 */
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			nameList.clear();
			ageList.clear();
			
			/**
			 * 把values中的来自于第一张表和第二张表的value进行区分
			 * 分别存储在nameList和ageList
			 */
			for(Text t: values){
				String trueValue = t.toString();
				String[] split = trueValue.split("--");
				if(split[0].equals("a")){
					nameList.add(split[1]);
				}else{
					ageList.add(split[1]);
				}
			}
			
			/**
			 * join的真正的实现的地方
			 */
			for(String name: nameList){
				for(String age: ageList){
					valueOut.set(name + "\t" + age);
					context.write(key, valueOut);
				}
			}
		}
	}
}

















