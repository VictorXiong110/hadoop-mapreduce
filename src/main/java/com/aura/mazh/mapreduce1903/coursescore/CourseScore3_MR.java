package com.aura.mazh.mapreduce1903.coursescore;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CourseScore3_MR {

	public static void main(String[] args) throws Exception {

		// 1 参数
		String inputPath = "D:\\testdata\\cs\\input\\";
		String outputPath = "D:\\testdata\\cs\\output_08\\";

		// 2 job对象
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 3 设置jar包路径
		job.setJarByClass(CourseScore3_MR.class);

		// 4 设置各种逻辑处理组件
		job.setMapperClass(CSMapper.class);
		job.setReducerClass(CSReducer.class);
		job.setMapOutputKeyClass(CourseScore.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(CourseScore.class);
		job.setOutputValueClass(NullWritable.class);
		
		
		job.setGroupingComparatorClass(CourseScoreComparator.class);

		// 5 设置输入
		FileInputFormat.addInputPath(job, new Path(inputPath));

		// 6 设置输出
		Path output = new Path(outputPath);
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			if (fs.exists(output)) {
				fs.delete(output, true);
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		FileOutputFormat.setOutputPath(job, output);

		// 7 任务提交
		boolean isDone = job.waitForCompletion(true);
		System.exit(isDone ? 0 : -1);

	}

	static class CSReducer extends Reducer<CourseScore, NullWritable, CourseScore, NullWritable> {

		/**
		 * key:  math
		 * values:  math这个课程的所有参加考试的学生的信息
		 * 
		 * key: math一组中，有多少个不同的学生，那么key的个数应该就有很多个
		 * 但是对于mapreduce来说：
		 * 
		 * reduce方法的参数： key相同的一组key-value
		 * 
		 * key: hello
		 * values: (1,1,1,1,1,1,1,1,1)
		 */
		@Override
		protected void reduce(CourseScore key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {

			/**
			 * 测试： 如果将来的结果文件当中，有多少行记录，就证明reduce方法被调用了多少次 也证明，总共有多少组key-values
			 */
			// 写死的方式：同一个key对象被重复输出了两次
			for(int i=0; i<=1; i++){
				context.write(key, NullWritable.get());
			}
			
			List<CourseScore> csList = new ArrayList<CourseScore>();
			
			// 动态的遍历key-value  每次输出的key
			int counter = 0;
			for(NullWritable nvl: values){
				context.write(key, NullWritable.get());
				counter++;
				CourseScore newCS = new CourseScore();
				try {
					BeanUtils.copyProperties(newCS, key);
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
				csList.add(newCS);
				if(counter == 2){
					break;
				}
			}
			
			for(CourseScore cs:  csList){
				System.out.println(cs.toString());
			}
			
			context.write(new CourseScore("------------------------------------","------", 0.0), NullWritable.get());
		}
	}

	static class CSMapper extends Mapper<LongWritable, Text, CourseScore, NullWritable> {

		// 关于输出的key-value的对象的初始化的问题
		CourseScore cs = new CourseScore();

		/**
		 * math,xuzheng,54,52,86,91,42,85,75
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] split = value.toString().split(",");
			String course = split[0];
			String name = split[1];

			double totalScore = 0;
			for (int i = 2; i < split.length; i++) {
				totalScore += Integer.valueOf(split[i]);
			}
			double avgScore = totalScore / (split.length - 2);

			cs.setCourse(course);
			cs.setName(name);
			cs.setAvgScore(avgScore);

			context.write(cs, NullWritable.get());
		}
	}
}
