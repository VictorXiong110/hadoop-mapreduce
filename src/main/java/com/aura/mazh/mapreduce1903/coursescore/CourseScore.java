package com.aura.mazh.mapreduce1903.coursescore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 
 */
public class CourseScore implements WritableComparable<CourseScore>{

	private String course;	// 分组条件
	private String name;
	private Double avgScore;	// 排序条件
	public String getCourse() {
		return course;
	}
	public void setCourse(String course) {
		this.course = course;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Double getAvgScore() {
		return avgScore;
	}
	public void setAvgScore(Double avgScore) {
		this.avgScore = avgScore;
	}
	public CourseScore(String course, String name, Double avgScore) {
		super();
		this.course = course;
		this.name = name;
		this.avgScore = avgScore;
	}
	public CourseScore() {
		super();
		// TODO Auto-generated constructor stub
	}
	@Override
	public String toString() {
		return course + "\t" + name + "\t" + avgScore;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(course);
		out.writeUTF(name);
		out.writeDouble(avgScore);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.course = in.readUTF();
		this.name = in.readUTF();
		this.avgScore = in.readDouble();
	}
	
	
	/**
	 * 默认情况下：
	 * 	这个方法：
	 * 	    1、排序规则
	 *      2、分组规则
	 *     
	 *  按照成绩排序：
	 *  	先按照课程排序，按照成绩排序
	 *  
	 * 
	 *  注意问题：
	 *  	
	 *  	如果这个方法返回 0； 表示 this 对象  和  cs 对象是同一组
	 *  
	 * 
	 *  冲突：
	 *  1、我要的分组规则：  courese
	 *  2、实际的分组规则： couse + score
	 */
	@Override
	public int compareTo(CourseScore cs) {
		
		// this  compare  cs
		int courseDiff = this.getCourse().compareTo(cs.getCourse());
		if(courseDiff == 0){
			
			double scoreDiff = this.getAvgScore() - cs.getAvgScore();
			if(scoreDiff == 0){
				return 0;
			}else if(scoreDiff > 0){
				return -1;
			}else{
				return 1;
			}
		}else{
			
			int result = (courseDiff > 0) ? 1 : -1;
			return result;
		}
	}
	
	
}
