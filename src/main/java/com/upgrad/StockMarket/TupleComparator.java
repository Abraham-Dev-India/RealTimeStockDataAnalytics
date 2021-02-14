package com.upgrad.kafkaspark;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

//Class that compares values of a tuple
public class TupleComparator implements Comparator<Tuple2<String, Double>>,
		Serializable {

	private static final long serialVersionUID = 1L;

	public int compare(Tuple2<String, Double> x, Tuple2<String, Double> y) {
		return Double.compare(x._2(), y._2());
	}

}