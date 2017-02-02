package com.alangiu.bigdata.spark;

import java.io.Serializable;

public class Count implements Serializable {
	
	public Integer total = 0;
	public Integer critical = 0;
	
	public Count(Integer total, Integer critical) {
		this.total = total;
		this.critical = critical;
	}
	
}
