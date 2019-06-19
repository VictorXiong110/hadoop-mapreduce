package com.aura.mazh.mapreduce1903.newapp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducerNew extends Reducer<Text, IntWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        long sum = 0;
        for(IntWritable i :  values){
            sum += i.get();
        }

        // key : key ，   value : sum
        LongWritable outVlaue = new LongWritable(sum);
        context.write(key, outVlaue);
    }
}
