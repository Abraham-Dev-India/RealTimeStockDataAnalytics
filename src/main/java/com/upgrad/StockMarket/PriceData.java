package com.upgrad.kafkaspark;

import java.io.Serializable;

public class PriceData implements Serializable {
		 private double close;
		 private double high;
		 private double low;
		 private double open;
		 private double volume;


		 // Getter Methods 

		 public double getClose() {
		  return close;
		 }

		 public double getHigh() {
		  return high;
		 }

		 public double getLow() {
		  return low;
		 }

		 public double getOpen() {
		  return open;
		 }

		 public double getVolume() {
		  return volume;
		 }

		 // Setter Methods 

		 public void setClose(double close) {
		  this.close = close;
		 }

		 public void setHigh(double high) {
		  this.high = high;
		 }

		 public void setLow(double low) {
		  this.low = low;
		 }

		 public void setOpen(double open) {
		  this.open = open;
		 }

		 public void setVolume(double volume) {
		  this.volume = volume;
		 }
		}