package com.aura.mazh.mapreduce1903.coursescore;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *  用来指定分组规则的
 *
 */
public class CourseScoreComparator extends WritableComparator{

	// 反序列化才有可能实现
	CourseScoreComparator(){
		super(CourseScore.class, true);
	}
	
	/**
	 * 如果你编写了 WritableComparator 这个组件，就证明你现在是在自定义分组规则
	 * 
	 * 方法的意义：
	 * 	 如果 aa == bb    =====  0 证明是同一组，
	 *   如果不是0 ， 就不是同一组
	 */
	@Override
	public int compare(WritableComparable a, WritableComparable b) {

		CourseScore aa = (CourseScore)a;
		CourseScore bb = (CourseScore)b;
		
		int diff = aa.getCourse().compareTo(bb.getCourse());
		
		return diff;
	}
	
}
