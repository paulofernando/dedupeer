package com.dedupeer.utils;

import com.dedupeer.dao.operation.UserFilesDaoOperations;

public class Utils {

	public static String getValidName(String filename) {
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		
		int count = 1;
		while(ufdo.fileExists(System.getProperty("username"), filename)) {
			count++;
			filename = FileUtils.getOnlyName(filename) + "(" + count + ")." + FileUtils.getOnlyExtension(filename);			
		}
		return filename;
	}
	
}
