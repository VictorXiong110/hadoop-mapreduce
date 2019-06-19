package com.aura.mazh.mapreduce1903.counter;

import org.apache.hadoop.util.ToolRunner;

public class CounterDriver {
	
	/**
	 * 在驱动程序中，定义了一个枚举类： 计数器
	 * @author Administrator
	 *
	 */
	enum MyCounter{
		Line_Counter,
		Word_Counter,
		GroupNumbers
	}

	public static void main(String[] args) {
		
		try {
			/**
			 * 这仅仅只是一个工具类
			 * 
			 * 经过各种嘟嘟转转 回到了去调用driver.run(args)
			 */
			WordCountCounterDriver driver = new WordCountCounterDriver();
			int isDone = ToolRunner.run(driver, args);
			
			System.exit(isDone);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
