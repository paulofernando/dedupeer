package deduplication;

import deduplication.checksum.Hashing;
import net.collegeman.rsync.checksum.MD5;

public class Main {
	
	public static void main (String[] args) {
		System.out.println(Hashing.getSHA1("Testando!"));
	}
	
}
