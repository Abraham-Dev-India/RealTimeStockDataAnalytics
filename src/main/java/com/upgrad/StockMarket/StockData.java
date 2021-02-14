package com.upgrad.kafkaspark;

import java.io.Serializable;

public class StockData implements Serializable {
	
		 private String symbol;
		 private String timestamp;
		 PriceData PriceDataObject;


		 // Getter Methods 

		 public String getSymbol() {
		  return symbol;
		 }

		 public String getTimestamp() {
		  return timestamp;
		 }

		 public PriceData getPriceData() {
		  return PriceDataObject;
		 }

		 // Setter Methods 

		 public void setSymbol(String symbol) {
		  this.symbol = symbol;
		 }

		 public void setTimestamp(String timestamp) {
		  this.timestamp = timestamp;
		 }

		 public void setPriceData(PriceData priceDataObject) {
		  this.PriceDataObject = priceDataObject;
		 }
		}
		

