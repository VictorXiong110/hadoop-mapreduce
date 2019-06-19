package com.aura.mazh.mapreduce1903.wcdemo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 执行汇总的逻辑
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

    /**
     * @param key   :    单词          hello
     * @param values    ：   这个单词所对应的的所有的value的集合   (1,1,1,1,1,1,1,1)
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
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
