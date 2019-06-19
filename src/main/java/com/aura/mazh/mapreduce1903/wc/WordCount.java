package com.aura.mazh.mapreduce1903.wc;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    // value就是读取到的一行
    /**
     * 就把 value 转换成 多个 (word,1)
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
     
      StringTokenizer itr = new StringTokenizer(value.toString());
//      String[] words = value.toString().split(" ");
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
      /*for(String word:  words){
    	  // 把mapper阶段算出来的临时结果交给下一个阶段：reducer
    	  context.write(word, one);
      }*/
    }
  }
  
  /**
   * 	从 
   * 	 context.write(word, one);
   *    之后
   *    到
   *    reduce(Text key, Iterable<IntWritable> values, Context context )
   *    执行之前
   *    
   *    这个完整的我们不知道的过程，就是shuffle 
   *    任何的分布式计算引擎，都有shuffle的概念和过程。
   *    
   *    这个shuffle也是最影响应用程序执行效率的地方
   * 
   *
   */
  
  public static class IntSumReducer  extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    /**
     * key:  单词
     * values:  就是这个单词所对应的的所有的1
     * 
     * reduce方法接收的参数，就是key相同的所有的value
     * 
     * reduce方法的参数：
     * 	（hello， (1,1,1,1,1,1)）
     */
    public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {
      
    	// sum 就是  values 中的所有的 1 的总和
    	int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      
      
      result.set(sum);
      
      /**
       * key ： 单词
       * result:  单词的出现次数
       */
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
	  
	  /**
	   * 一些参数的初始化
	   */
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
//    	                  Usage: wordcount <in> [<in>...] <out>
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }
    
    
    /**
     * 初始化一个Job对象
     */
    Job job = Job.getInstance(conf, "word count");
    
    
    
   /**
    * 针对job对象进行各种业务组件的设置
    */
    job.setJarByClass(WordCount.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    // 逻辑处理组价
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    
    
    /**
     * 指定输出数据的目录
     */
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    
    
    /**
     * 指定输出数据的目录
     */
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    
    
    
    /**
     * job.submit()
     * 提交任务执行    一键提交
     */
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
