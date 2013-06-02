package com.dedupeer.utils;

public class Range implements Comparable<Range> {
	
	private long initialValue, finalValue;
	
	public Range(long intialValue, long finalValue) {
		this.initialValue = intialValue;
		this.finalValue = finalValue;
	}

	public long getInitialValue() {
		return initialValue;
	}

	public long getFinalValue() {
		return finalValue;
	}

	@Override
	public int compareTo(Range range) {
	    if(this.initialValue > range.getInitialValue()) {
	    	return 1;
	    } else  if(this.initialValue == range.getInitialValue()) {
	    	return 0;
		} else {
			return -1;
		}
	}

	public void setInitialValue(long initialValue) {
		this.initialValue = initialValue;
	}

	public void setFinalValue(long finalValue) {
		this.finalValue = finalValue;
	}
}
