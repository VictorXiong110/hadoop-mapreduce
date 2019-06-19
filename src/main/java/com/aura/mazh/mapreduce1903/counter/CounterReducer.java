package com.aura.mazh.mapreduce1903.counter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.aura.mazh.mapreduce1903.counter.CounterDriver.MyCounter;

import java.io.IOException;

public class CounterReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

    	context.getCounter(MyCounter.GroupNumbers).increment(1);
    	
        long sum = 0;
        for(IntWritable i :  values){
            sum += i.get();
        }

        // key : key ，   value : sum
        LongWritable outVlaue = new LongWritable(sum);
        context.write(key, outVlaue);
    }
}
