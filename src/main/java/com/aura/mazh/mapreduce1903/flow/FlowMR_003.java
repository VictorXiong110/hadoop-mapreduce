package com.aura.mazh.mapreduce1903.flow;

import java.io.IOException;

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

/**
 * 3、 将流量汇总统计结果按照手机归属地不同省份输出到不同文件中
 * 
 * 最终代码编写中需要做的事情：
 * 
 * 自定义分区规则
 */
public class FlowMR_003 {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		// 1 参数
		String inputPath = "D:\\testdata\\flow\\output25\\";
		String outputPath = "D:\\testdata\\flow\\output29\\";

		// 2 job对象
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 3 设置jar包路径
		job.setJarByClass(FlowMR_003.class);

		// 4 设置各种逻辑处理组件
		job.setMapperClass(Flow3Mapper.class);
		job.setReducerClass(Reducer.class);
		// 如果mapper阶段输出的key-value类型和reducer阶段输出的keyu-value类型一致，就可以不用
		// 写mapper阶段的keyvalue类型
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(FlowBean.class);
		job.setOutputValueClass(NullWritable.class);

		
		/**
		 * 数据的分区规则没有指定:
		 * 默认实现：HashParitioner
		 * 
		 * 常用的分区规则有哪些？
		 */
		job.setPartitionerClass(CityPartitioner.class);
		job.setNumReduceTasks(7);
		// 0 1 2 3 4 5
		

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

	static class Flow3Mapper extends Mapper<LongWritable, Text, FlowBean, NullWritable> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, FlowBean, NullWritable>.Context context)
				throws IOException, InterruptedException {

			String[] fields = value.toString().split("\t");

			String phone = fields[0];
			long upFlow = Long.valueOf(fields[1]);
			long downFlow = Long.valueOf(fields[2]);
			long sumFlow = Long.valueOf(fields[3]);

			FlowBean outKey = new FlowBean(upFlow, downFlow, sumFlow, phone);
			context.write(outKey, NullWritable.get());
		}
	}

	// 由于不需要再reducer阶段指定任何的聚合逻辑，所以使用默认的Reducer组件即可
}
