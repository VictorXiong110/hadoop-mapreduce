package com.aura.mazh.mapreduce1903.coursescore;

import java.util.ArrayList;
import java.util.List;

public class StudentTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		List<Student> slist = new ArrayList<Student>();
		
		
		
		for(int i=1; i<=5; i++){
			Student s = new Student();
			s.setId(i);
			s.setName("a" + i);
			slist.add(s);
		}
		
		for(Student ss: slist){
			System.out.println(ss.toString());
		}
		
	}

}

/**
 * @author Administrator
 *
 */
class Student{
	
	public Student(){}
	private int id;
	private String name;
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	@Override
	public String toString() {
		return id + "\t" + name;
	}
	public Student(int id, String name) {
		super();
		this.id = id;
		this.name = name;
	}
	
	
}
