package com.aura.mazh.mapreduce1903.counter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import com.aura.mazh.mapreduce1903.counter.CounterDriver.MyCounter;

import java.io.IOException;

public class CounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
   
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 重新实现自己的逻辑
    	
    	// 使用计数器来统计行数
    	Counter counter = context.getCounter(MyCounter.Line_Counter);
    	counter.increment(1);

        String[] words = value.toString().split(" ");
        
    	
        for(String word : words){

        	Counter wordCounter = context.getCounter(MyCounter.Word_Counter);
        	wordCounter.increment(1);
        	
            // 单词
            Text outKey = new Text();
            outKey.set(word);
            // 1次
            IntWritable one = new IntWritable(1);
            context.write(outKey, one);
        }
    }
}
