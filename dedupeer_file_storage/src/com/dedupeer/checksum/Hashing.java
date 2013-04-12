package com.dedupeer.checksum;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import com.dedupeer.utils.GeneralUtils;


/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 *  @deprecated
 */
public class Hashing {
	
	/**
	 * Computes the SHA-1 of a dataset
	 * @param data Dataset to compute the hash
	 * @return The SHA-1 represented how Hexadecimal
	 */
	public static String getSHA1(byte[] data) {
		byte[] sha1hash;
		try {
			MessageDigest md = MessageDigest.getInstance("SHA1");
			sha1hash = new byte[40];
			md.update(data);
			sha1hash = md.digest();
			return GeneralUtils.toHex(sha1hash);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}		
		return null;		
	}
		
	/**
	 * Computes the Alder32 of a dataset
	 * @param data Dataset to compute the hash
	 * @return The hash value
	 */
	public static long getAlder32(byte[] data) {
		Checksum checksum = new Adler32();
		checksum.update(data, 0, data.length);
		return checksum.getValue();
	}
	
}
