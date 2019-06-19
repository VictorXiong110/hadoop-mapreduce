package com.aura.mazh.mapreduce1903.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

class FlowBean implements WritableComparable<FlowBean>{
	
	private long upFlow;
	private long downFlow;
	private long sumFlow;
	private String phone;
	public long getUpFlow() {
		return upFlow;
	}
	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}
	public long getDownFlow() {
		return downFlow;
	}
	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}
	public long getSumFlow() {
		return sumFlow;
	}
	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}
	public String getPhone() {
		return phone;
	}
	public void setPhone(String phone) {
		this.phone = phone;
	}
	public FlowBean(long upFlow, long downFlow, long sumFlow, String phone) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = sumFlow;
		this.phone = phone;
	}
	public FlowBean() {
		super();
		// TODO Auto-generated constructor stub
	}
	@Override
	public String toString() {
		return sumFlow + "\t" + downFlow + "\t" + upFlow + "\t" + phone;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
		out.writeUTF(phone);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.upFlow = in.readLong();
		this.downFlow = in.readLong();
		this.sumFlow = in.readLong();
		this.phone = in.readUTF();
	}
	
	
	/**
	 * 这个方法， 在默认情况， 即是 排序规则    也是  分组规则
	 * 
	 * -1   1   0    决定排序规则
	 * == 0    != 0  分组
	 * 
	 * x.compareTo(y) = (1, -1, 0)
	 * 
	 * 1: 降序
	 * -1 ： 升序
	 * 
	 * == 0 : 同一组
	 * != 0 : 不同的组
	 */
	@Override
	public int compareTo(FlowBean o) {
		
		// this  和   o  
		FlowBean a = (FlowBean)o;
		
		long diff = this.getSumFlow() - a.getSumFlow();
		
		if(diff > 0){
			// 降序
			return -1;
		}else if(diff < 0){
			// 升序
			return 1;
		}else{
			// 相等
			return 0;
		}
	}
}