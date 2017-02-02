package com.alangiu.bigdata.spark;

import java.io.Serializable;

public class Review implements Serializable {
	
	public String prod_id;
	public Double rating;
	
	public Review(String prod_id, Double rating) {
		this.prod_id = prod_id;
		this.rating = rating;
	}

}
