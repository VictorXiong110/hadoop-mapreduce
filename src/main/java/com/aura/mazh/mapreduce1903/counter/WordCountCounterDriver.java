package com.aura.mazh.mapreduce1903.counter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;

public class WordCountCounterDriver extends Configured implements Tool{

	/**
	 * 现在这个run方法中的所有逻辑，就是driver中的main方法中的所有逻辑
	 * 
	 * 参数： 等同于原来的main方法的参数
	 * 返回值： 最后提交之后，任务返回的结果值
	 */
	@Override
	public int run(String[] args) throws Exception {
        
		String intputPath = "c:/wc/input/";
        String outputPath = "c:/wc/output_counter1/";

        Configuration conf = new Configuration();
        Job job = null;
        try {
            job = Job.getInstance(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        job.setJarByClass(WordCountCounterDriver.class);

        job.setMapperClass(CounterMapper.class);
        job.setReducerClass(CounterReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        job.setInputFormatClass(TextInputFormat.class); // 这个组件顺带的帮我们指定了输入的key-value的类型

        job.setPartitionerClass(HashPartitioner.class);	// 写和没写是一样的，默认的实现，默认的机制
        
        try {
            FileInputFormat.addInputPath(job, new Path(intputPath));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Path output = new Path(outputPath);
        FileOutputFormat.setOutputPath(job, output);
        boolean isDone = false;
        try {
        	isDone = job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        } 
        return isDone ? 0 : -1;
	}
}
