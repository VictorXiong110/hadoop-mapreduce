package com.aura.mazh.mapreduce1903.wcdemo;

import java.io.IOException;

/**
 * 关于导报的问题
 * 
 * 
 * 1、mapred 中的类    old api
 * 2、mapreduce  新 api
 * 
 * 以前写代码的习惯：
 * 	class XXX implements SomeInterface
 * 	优点：会提醒你，那些方法，需要实现
 *  缺点： 一定要实现
 * 
 * 现在：
 * 	class XXX extends SomeAbstractClass
 *   优点：会有默认实现
 *   缺点：开发者有可能会忽略这个方法
 * 
 *  SomeAbstractClass implements SomeInterface
 *  
 *  void run();
 *  void run(){}
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 *  全限定名
 * com.aura.mazh.wc.WordCountDriver
 */
public class WordCountDriver {

    public static void main(String[] args) {

        /**
         * 解析参数
         */
        String intputPath = "c:/wc/input/";
        String outputPath = "c:/wc/output12/";
//        String intputPath = args[0];
//        String outputPath = args[1];


        /**
         * 获取job对象
         *  创建对象的方式有几种：
         *      5种：
         *      1、new的方式调用构造器
         *      2、调用静态工厂方法
         *      3、反射
         *      4、克隆
         *      5、反序列化
         */
        Configuration conf = new Configuration();
        Job job = null;
        try {
            job = Job.getInstance(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }


        /**
         * 设置当前这个类所在的jar包的存储位置
         *  路径写死
         */
        //job.setJar("c:/mapreduce1903-1.0-SNAPSHOT.jar");
        job.setJarByClass(WordCountDriver.class);



        /**
         * 设置job的各种处理组件
         */
        job.setMapperClass(WordCountMapper.class);
        // 泛型擦除的问题
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 你只指定了mapper的输出的key-value类型，没有指定输入的key-value的类型
        // 因为输入的key-value类型，已经被输入组件给确定了
        // 默认的数据读取组件是：TextInputFormat: 逐行读取
        job.setInputFormatClass(TextInputFormat.class); // 这个组件顺带的帮我们指定了输入的key-value的类型

        
        job.setNumReduceTasks(0);
        /**
         * 如果 job.setNumReduceTasks(0)
         * 那么这个reducer组件就不执行， mapper阶段执行完，就直接输出得到最终结果了
         */
//        job.setReducerClass(WordCountReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(LongWritable.class);

        /**
         * 在你不修改默认的Partitioner的情况下，numReduceTask的个数据没有限制
         * 
         * Partitioner的作用是干什么的？ 我们怎么使用？
         * 
         * 默认的实现： HashPartitioner
         */
        job.setPartitionerClass(HashPartitioner.class);	// 写和没写是一样的，默认的实现，默认的机制
        
        
        /**
         * n
         * 
         * 1、n > 1  设置成几个reduceTask，最终就有几个结果文件
         * 2、n == 1  表示只有一个reduceTask, job.setParitionerclass 就不起作用了
         * 				分区器没有作用过了
         * 3、n == 0 呢？
         * 			表示只有mapper阶段，没有reducer阶段
         */
//        job.setNumReduceTasks(2);
        
        
        

        /**
         * 指定输入
         */
        try {
            FileInputFormat.addInputPath(job, new Path(intputPath));
        } catch (IOException e) {
            e.printStackTrace();
        }


        /**
         * 指定输出
         */
        Path output = new Path(outputPath);
        /*FileSystem fs = null;
        try {
        	fs = FileSystem.get(conf);
        	if(fs.exists(output)){
            	fs.delete(output, true);
            }
		} catch (IOException e1) {
			e1.printStackTrace();
		}*/
        
        FileOutputFormat.setOutputPath(job, output);


        /**
         * 任务提交
         * true  verbos   打印任务的执行进度
         */
        try {
            // 阻塞方法
            boolean isDone = job.waitForCompletion(true);

            // 推出Driver程序
            System.exit(isDone ? 0 : -1);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }
}
