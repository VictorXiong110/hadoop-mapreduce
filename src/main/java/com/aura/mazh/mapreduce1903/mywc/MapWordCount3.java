package com.aura.mazh.mapreduce1903.mywc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 需求：统计一本小说中的，每个单词出现了多少次
 * 
 * 对于1M的小说来说，没问题
 * 
 * 但是对于 100T 的数据问题的计算：
 * 	  慢
 *   根本计算不出来
 */
public class MapWordCount3 {
	
	// 数据目录
	static String filePath = "D:\\wc\\hadoop04\\wc_part03.txt";
	
	static BufferedReader br = null;
	
	// 存储统计结果的 key 就是单词，  value 这个单词出现的次数
	static Map<String,Integer> wordCountMap = new HashMap<String, Integer>();
	
	// 每次读取到的那一行的值
	static String line = null;
	
	public static void init(){
		
		try {
			br = new BufferedReader(new InputStreamReader
					(new FileInputStream(new File(filePath))));
		} catch (FileNotFoundException e) {
			System.out.println("读取文件出错");
		}
	}

	// 这是最终的结果输出目录
			static String outputDir = "D://result_wc_1903//";

	public static void main(String[] args) {
			
			// 初始化资源和连接的
			init();
			
			/**
			 * 逻辑处理：
			 * 每次判断是否能读取到一行数据，然后处理一下
			 * 
			 * line:  存储每次读取到的一行
			 * wordCountMap :  存储处理得到的临时结果
			 */
			while(nextLine()){
				handleLine(line);
			}
			
			// 结果处理
			// 打印输出
//			printMap(wordCountMap);
			// 存储到文件系统中的文件中去
			saveAsTextFile(outputDir, wordCountMap);
			
			// 关闭连接，回收资源
			close();
			
		}
		
		// 存储到文件系统中的文件中去
		private static void saveAsTextFile(String outputDir, Map<String, Integer> wcMap) {
			
			String fileName = "result3";
			PrintWriter pw = null;
			try {
				pw = new PrintWriter(new File(outputDir + "//" + fileName));
				
				Set<String> keySet = wcMap.keySet();
				for(String key: keySet){
					pw.println(key+"----"+wcMap.get(key));
				}
				
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} finally{
				pw.close();
			}
		}
	
	// 关闭连接，回收资源
	public static void close(){
		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// 处理结果的逻辑方法
	public static void printMap(Map<String,Integer> wcMap){
		
		Set<String> keySet = wcMap.keySet();
		for(String key: keySet){
			
			System.out.println(key + " : " + wcMap.get(key));
		}
	}
	
	// 每次读取一行
	public static String readLine() throws Exception{
		return line;
	}
	
	// 把读取到的一行数据，处理一下，把统计结果汇总到 HashMap
	public static void handleLine(String line){
		
		String[] words = line.split(" ");
		for(String word:  words){
			
			boolean containsKey = wordCountMap.containsKey(word);
			
			if(containsKey){
				wordCountMap.put(word, wordCountMap.get(word) + 1);
			}else{
				wordCountMap.put(word, 1);
			}
		}
	}
	
	
	
	// 判断有没有下一行
	/**
	 * 这个方法两个作用：
	 * 1、用来判断是否有下一行
	 * 2、如果这个方法执行，并且能读取到数据的话，那么数据已经被读取到line这个变量中了
	 * @return
	 * @throws Exception
	 */
	public static boolean nextLine(){
		
		try {
			line = br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if(line == null) return false; 
		return true;
	}
}
