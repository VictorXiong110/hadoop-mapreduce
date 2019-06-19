package com.aura.mazh.mapreduce1903.cf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 	这个是驱动程序，一次调度两个Job按照依赖关系先后运行
 *
 */
public class JobControl_CF {

	public static void main(String[] args) throws Exception {
		
		/**
		 * JOB1 的声明
		 */
		String intputPath = args[0];
        String outputPath = args[1];
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        
        job.setJarByClass(JobControl_CF.class);
        job.setMapperClass(CF_Mapper1.class);
        job.setReducerClass(CF_Reducer1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(intputPath));
        Path output = new Path(outputPath);
        FileOutputFormat.setOutputPath(job, output);
		
        
        /**
		 * JOB2 的声明
		 */
		String intputPath2 = args[1];
        String outputPath2 = args[2];
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2);
        job2.setJarByClass(JobControl_CF.class);
        job2.setMapperClass(CF_Mapper2.class);
        job2.setReducerClass(CF_Reducer2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(intputPath2));
        Path output2 = new Path(outputPath2);
        FileOutputFormat.setOutputPath(job2, output2);
		
        
        /**
         * job3
         */
        
        /**
         * 重头戏：串起来
         * JobControl类的作用：
         * 	专门启动一个线程用来监控所有job的执行状态和管理依赖关系
         */
        JobControl jc  = new JobControl("cfmr");
        
        // 必须转换job成为 ControlledJob 对象
        ControlledJob cjob1 = new ControlledJob(job.getConfiguration());
        ControlledJob cjob2 = new ControlledJob(job2.getConfiguration());
        
        // 指定依赖
        cjob2.addDependingJob(cjob1);
        
        // 添加要运行的job
        jc.addJob(cjob1);
        jc.addJob(cjob2);
        
        
        Thread jcThread = new Thread(jc);
        jcThread.start();	// 提交任务执行
        
        /**
         * 每隔 1s 钟 询问，整个app  是否完成计算
         */
        boolean allFinished = jc.allFinished();
        while(!allFinished){
        	Thread.sleep(1000);
        	allFinished = jc.allFinished();
        }
        
        System.exit(0);
	}
}














