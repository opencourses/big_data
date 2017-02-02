package com.alangiu.bigdata.spark;

import java.io.Serializable;

public class Count implements Serializable {

	public Integer total = 0;
	public Integer cheap = 0;
	
	public Count(Integer total, Integer cheap) {
		this.total = total;
		this.cheap = cheap;
	}
	
}
