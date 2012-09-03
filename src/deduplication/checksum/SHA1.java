package deduplication.checksum;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import deduplication.Utils;

public class SHA1 {
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
	
}
