package com.aura.mazh.mapreduce1903.newapp;

import org.apache.hadoop.util.ToolRunner;

public class WordCountRunner {

	public static void main(String[] args) {
		
		try {
			/**
			 * 这仅仅只是一个工具类
			 * 
			 * 经过各种嘟嘟转转 回到了去调用driver.run(args)
			 */
			WordCountNewAPP driver = new WordCountNewAPP();
			int isDone = ToolRunner.run(driver, args);
			
			System.exit(isDone);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
