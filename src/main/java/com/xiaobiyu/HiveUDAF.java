package com.xiaobiyu;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class HiveUDAF extends UDAF{

	public static class PhoneAmount implements UDAFEvaluator{

		private IntWritable amount = new IntWritable(0);
		
		public void init() {
			
		}
		
		public boolean iterate(Text str){
			if(str != null){
				int i = amount.get();
				i++;
				amount.set(i);
			}
			return true;
		}
		
		public IntWritable terminatePartial(){
			return amount;
		}
		
		public boolean merge(IntWritable other){
			int i = amount.get();
			int j = other.get();
			amount.set(i+j);
			return true;
		}
		
		public IntWritable terminate(){
			return amount;
		}
	}
}
