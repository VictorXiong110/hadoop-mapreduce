package com.aura.mazh.mapreduce1903.wcdemo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN, VALUEIN, KEYOUT, VALUEOUT
 *
 *
 * KEYIN, VALUEIN  传送给map方法的两个参数的类型
 * KEYOUT, VALUEOUT  mapper阶段输出给reduer阶段的key-value的类型
 *
 * 默认的数据读取组件是  TextInputFormat和 LineRecordReader
 * 读取到的数据会封装成keyu-value的形式：
 *  key: 这一行字符串在整个文件中的 起始偏移量
 *  value: 逐行读取时读取到的一整行数据
 *
 *  给mapper组件指定泛型的时候，一定要注意， 因为这些类型，都是要进行序列化的
 *  String   Text
 *  int       IntWritable
 *  long      LongWritable
 *
 *  ...
 *  Student implemnets Writable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


    /**
     * @param key   :   偏移量
     * @param value ：  读取的一行数据
     * @param context   ：   负责数据的流转    write(key， value)
     * @throws IOException
     * @throws InterruptedException
     */
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

            /**
             * 在mapper阶段被写出这个key-value之后，马上调用partitioner.getParition(key, value, numReduceTasks)
             * 得到一个分区编号
             * ptnNumber-outkey, value
             */
            context.write(outKey, one);
        }

    }
}
