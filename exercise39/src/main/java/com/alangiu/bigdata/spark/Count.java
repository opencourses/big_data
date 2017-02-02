package com.alangiu.bigdata.spark;

import java.io.Serializable;

public class Count implements Serializable {

	public int total;
	public int critical;
	
	public Count(int total, int critical) {
		this.total = total;
		this.critical = critical;
	}
	
}
