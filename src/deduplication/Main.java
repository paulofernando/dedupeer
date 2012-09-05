package deduplication;

import deduplication.checksum.Hashing;
import deduplication.checksum.RollingAlder32;
import deduplication.processing.EagleEye;
import net.collegeman.rsync.checksum.MD5;

public class Main {
	
	public static void main (String[] args) {
		//System.out.println(Hashing.getSHA1("Testando!".getBytes()));
		//RollingAlder32.rollingIn("Testando a parada aqui pra ver se está na paz de Jah".getBytes(), 0, 10);
		EagleEye.duplicationIdentification("Testando a parada aqui".getBytes(), "tando a".getBytes());
	}
	
}
