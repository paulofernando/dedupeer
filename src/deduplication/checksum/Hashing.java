package deduplication.checksum;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import deduplication.Utils;

public class Hashing {
	
	/**
	 * Computes the SHA-1 of a dataset
	 * @param data Dataset to compute the hash
	 * @return The SHA-1 represented how Hexadecimal
	 */
	public static String getSHA1(String data) {
		byte[] sha1hash;
		try {
			MessageDigest md = MessageDigest.getInstance("SHA1");
			sha1hash = new byte[40];
			md.update(data.getBytes());
			sha1hash = md.digest();
			return Utils.toHex(sha1hash);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}		
		return null;		
	}
	
	
	/**
	 * Computes the MD5 of a dataset
	 * @param data Dataset to compute the hash
	 * @return The MD5 represented how Hexadecimal
	 */
	public static String getMD5(String data) {		
		byte[] sha1hash;
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			sha1hash = new byte[40];
			md.update(data.getBytes());
			sha1hash = md.digest();
			return Utils.toHex(sha1hash);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}		
		return null;		
	}
	
}
