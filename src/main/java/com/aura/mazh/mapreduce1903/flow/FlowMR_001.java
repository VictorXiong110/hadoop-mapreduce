package com.aura.mazh.mapreduce1903.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 	期望一次成功
 *  但是成功不了
 *  
 *  hadoop jar mapreduce1903_002.jar com.aura.mazh.mapreduce1903.flow.FlowMR_001 /flow1903/input/ /flow1903/output/ fasle
 */
public class FlowMR_001 {

	public static void main(String[] args) throws Exception {

		// 1 参数
//		String inputPath = "D:\\testdata\\flow\\input\\";
//		String outputPath = "D:\\testdata\\flow\\output25\\";
		String inputPath = args[0];
		String outputPath = args[1];
		String combiner = args[2];	// true  false
		
		// 2 job对象
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		// 3 设置jar包路径
		job.setJarByClass(FlowMR_001.class);
		
		// 4 设置各种逻辑处理组件
		job.setMapperClass(Flow1Mapper.class);
		job.setReducerClass(Flow1Reducer.class);
		// 如果mapper阶段输出的key-value类型和reducer阶段输出的keyu-value类型一致，就可以不用
		// 写mapper阶段的keyvalue类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Flow.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Flow.class);
		
		boolean flag = Boolean.valueOf(combiner);
		if(flag){
			job.setCombinerClass(Flow1Reducer.class);
		}else{
			
		}
		
		job.setNumReduceTasks(1);
		
		// 5 设置输入
		FileInputFormat.addInputPath(job, new Path(inputPath));
		
		// 6 设置输出
		Path output = new Path(outputPath);
		FileSystem fs = null;
        try {
        	fs = FileSystem.get(conf);
        	if(fs.exists(output)){
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
	
	
	
	static class Flow1Reducer extends Reducer<Text, Flow, Text, Flow>{
		
		/**
		 * reduce接收的参数是：
		 * key； 手机号
		 * values : 这个手机号所对应的一堆Flow对象
		 */
		@Override
		protected void reduce(Text key, Iterable<Flow> values, Reducer<Text, Flow, Text, Flow>.Context context)
				throws IOException, InterruptedException {
			
			long sumUpFlow = 0;
			long sumDownFlow = 0;
			
			for(Flow f : values){
				sumUpFlow += f.getUpFlow();
				sumDownFlow += f.getDownFlow();
			}
			
			Flow lastOutValueFlow = new Flow(sumUpFlow, sumDownFlow);
			
			context.write(key, lastOutValueFlow);
			
		}
	}
	
	
	class Flow1Combiner extends Reducer<Text, Flow, Text, Flow>{
		
		@Override
		protected void reduce(Text key, Iterable<Flow> values, Reducer<Text, Flow, Text, Flow>.Context context)
				throws IOException, InterruptedException {
			
			long sumUpFlow = 0;
			long sumDownFlow = 0;
			
			for(Flow f : values){
				sumUpFlow += f.getUpFlow();
				sumDownFlow += f.getDownFlow();
			}
			
			Flow lastOutValueFlow = new Flow(sumUpFlow, sumDownFlow);
			
			context.write(key, lastOutValueFlow);
			
		}
	}

}


class Flow1Mapper extends Mapper<LongWritable, Text, Text, Flow>{
	
	/**
	 * 1363157984040 13602846565 5C-0E-8B-8B-B6-00:CMCC 120.197.40.4 2052.flash2-http.qq.com 综合门户 15 12 1938 2910 200
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Flow>.Context context)
			throws IOException, InterruptedException {
		
		String[] fields = value.toString().split("\t");
		/*if(fields.length < 11){
			return;
		}*/
		String phone_key = fields[1];
		Text outKey = new Text(phone_key);
		
		long upFlow = Integer.valueOf(fields[fields.length - 3]);
		long downFlow = Integer.valueOf(fields[fields.length - 2]);
		Flow flow_value = new Flow(upFlow, downFlow);
		
		context.write(outKey, flow_value);
	}
}


/**
 * 问题：
 * 
 * 	如果 Flow 类型，实现 Serializable 接口，就可以实现序列化
 *  而是使用 implements Writable 的方式呢？
 *  
 *  1、原生的java自带的序列化方式，太重，除了携带当前这个对象中的属性的值得信息意外，
 *  还会携带很多的关于这个类型的信息
 *  
 *  2、只传送对应的对象中的属性值得信息，每个task在接收这个序列化信息的对象时，只会接收类型信息一次
 *  网络负载就减小了。
 *  
 *  新的实现序列化的机制：
 *  	String     		Text
 *  	Int				IntWritable
 *      自定义类型 		implements Writable
 *      
 *  这种序列化方式：
 *  	1、通过反射创建对象
 *  	 	在序列化的时候，会传送给每个task 这个类型信息一次。
 *  		拿到了类型信息，就可以通过反射去创建对象, 对象的所有属性值都是默认的
 * 
 * 
 * 		2、通过反序列化给当前这些对象设置属性的值
 */

class Flow implements Writable{
	
	private long upFlow;		// 上行流量
	private long downFlow;		// 下行流量
	private long sumFlow;		// 总流量
	
	public long getUpFlow() {
		return upFlow;
	}
	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}
	public long getDownFlow() {
		return downFlow;
	}
	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}
	public long getSumFlow() {
		return sumFlow;
	}
	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}
	public Flow(long upFlow, long downFlow, long sumFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = sumFlow;
	}
	public Flow(long upFlow, long downFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}
	public Flow() {
		super();
	}
	
	
	
	@Override
	public String toString() {
		return upFlow + "\t" + downFlow + "\t" + sumFlow;
	}
	/**
	 * 序列化方法
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}
	
	
	/**
	 * 反序列化
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upFlow = in.readLong();
		this.downFlow = in.readLong();
		this.sumFlow = in.readLong();
	}
	
}