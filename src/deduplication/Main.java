package deduplication;

import deduplication.checksum.SHA1;
import net.collegeman.rsync.checksum.MD5;

public class Main {
	
	public static void main (String[] args) {
		System.out.println(SHA1.getSHA1("Testando!"));
	}
	
}
