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
 * 这个程序的目的，就是把之前，所有的那些计算每个文件的临时结果 汇总到一起
 */
public class ReduceWordCount {

	static String filePath = "D:\\result_wc_1903";

	// 用来存储最终汇总的结果的 把那三个结果文件的数据汇总到一个文件里
	static Map<String, Integer> lastResultMap = new HashMap<>();

	public static void main(String[] args) throws IOException {

		try {
			reduceFile();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		saveAsTextFile("d://lastResult//", lastResultMap);
	}

	public static void reduceFile() throws Exception{

		BufferedReader br = null;
		
		File file = new File(filePath);
		File[] listFiles = file.listFiles();
		for(File f:  listFiles){
//			System.out.println(f.getPath());
			
			try {
				br = new BufferedReader(new InputStreamReader
						(new FileInputStream(f)));
				
				String line = null;
				while((line = br.readLine()) != null){
					
					String[] wordAndCount = line.split("----");
					String word = wordAndCount[0];
					Integer count = Integer.valueOf(wordAndCount[1]);
					
					// word存在
					if(lastResultMap.containsKey(word)){
						lastResultMap.put(word, lastResultMap.get(word) + count);
					}else{
						lastResultMap.put(word, count);
					}
				}
				
				br.close();
				
				
			} catch (FileNotFoundException e) {
				System.out.println("读取文件出错");
			}
			
		}
		
	}

	// 处理结果的逻辑方法
	public static void printMap(Map<String, Integer> wcMap) {

		Set<String> keySet = wcMap.keySet();
		for (String key : keySet) {

			System.out.println(key + " : " + wcMap.get(key));
		}
	}

	// 存储到文件系统中的文件中去
	private static void saveAsTextFile(String outputDir, Map<String, Integer> wcMap) {

		String fileName = "lastResult";
		PrintWriter pw = null;
		try {
			pw = new PrintWriter(new File(outputDir + "//" + fileName));

			Set<String> keySet = wcMap.keySet();
			for (String key : keySet) {
				pw.println(key + "----" + wcMap.get(key));
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			pw.close();
		}
	}
}

