package com.aura.mazh.mapreduce1903.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *  需求：按照总流量从大到小排序
 *
 */
public class FlowMR_002 {

	public static void main(String[] args) throws Exception {
		
		// 1 参数
		String inputPath = "D:\\testdata\\flow\\output25\\";
		String outputPath = "D:\\testdata\\flow\\output27\\";
		
		// 2 job对象
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		// 3 设置jar包路径
		job.setJarByClass(FlowMR_002.class);
		
		// 4 设置各种逻辑处理组件
		job.setMapperClass(Flow2Mapper.class);
		job.setReducerClass(Flow2Reducer.class);
		// 如果mapper阶段输出的key-value类型和reducer阶段输出的keyu-value类型一致，就可以不用
		// 写mapper阶段的keyvalue类型
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(FlowBean.class);
		job.setOutputValueClass(NullWritable.class);
		
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

	static class Flow2Mapper extends Mapper<LongWritable, Text, FlowBean, NullWritable>{
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, FlowBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			String[] fields = value.toString().split("\t");
			
			String phone = fields[0];
			long upFlow = Long.valueOf(fields[1]);
			long downFlow = Long.valueOf(fields[2]);
			long sumFlow = Long.valueOf(fields[3]);
			
			FlowBean outKey = new FlowBean(upFlow, downFlow, sumFlow, phone);
			context.write(outKey, NullWritable.get());
		}
	}
	
	static class Flow2Reducer extends Reducer<FlowBean, NullWritable, FlowBean, NullWritable>{
		
		@Override
		protected void reduce(FlowBean key, Iterable<NullWritable> values,
				Reducer<FlowBean, NullWritable, FlowBean, NullWritable>.Context context)
				throws IOException, InterruptedException {

			// 调用父类中的reduce方法
			super.reduce(key, values, context);
		}
	}
}



















