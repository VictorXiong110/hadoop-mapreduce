package com.aura.mazh.mapreduce1903.newapp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapperNew extends Mapper<LongWritable, Text, Text, IntWritable> {
   
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 重新实现自己的逻辑

        String[] words = value.toString().split(" ");
        for(String word : words){

            // 单词
            Text outKey = new Text();
            outKey.set(word);
            // 1次
            IntWritable one = new IntWritable(1);
            context.write(outKey, one);
        }
    }
}
