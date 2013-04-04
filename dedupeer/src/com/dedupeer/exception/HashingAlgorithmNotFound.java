package com.dedupeer.exception;

public class HashingAlgorithmNotFound extends Exception {

	private static final long serialVersionUID = 1L;
	
	public HashingAlgorithmNotFound() {
		super("The algorithm specified was not found");
	}
}
