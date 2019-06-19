package com.aura.mazh.mapreduce1903.cf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MR1 {

	public static void main(String[] args) {
		
		String intputPath = "D:\\testdata\\friend\\input";
        String outputPath = "D:\\testdata\\friend\\output\\";

        Configuration conf = new Configuration();
        Job job = null;
        try {
            job = Job.getInstance(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        job.setJarByClass(MR1.class);

        job.setMapperClass(CF_Mapper1.class);
        job.setReducerClass(CF_Reducer1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        
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
        System.exit(isDone ? 0 : -1);
	}
}
