package deduplication;

import deduplication.checksum.Hashing;
import deduplication.checksum.Rolling;
import deduplication.checksum.RollingAlder32;
import deduplication.processing.EagleEye;
import deduplication.utils.FileUtils;

public class Main {
	
	public static void main (String[] args) {
		//System.out.println(Hashing.getSHA1("Testando!".getBytes()));
		//RollingAlder32.rollingIn("Testando a parada aqui pra ver se está na paz de Jah".getBytes(), 0, 10);
		//EagleEye.duplicationIdentification(FileUtils.getBytesFromFile("D:/dedup.txt"), "tando a".getBytes());
		//EagleEye.duplicationIdentification(FileUtils.getBytesFromFile("D:/teste/teddy_picker_5s.mp3"), FileUtils.getBytesFromFile("D:/teste/teddy_picker_chunk.mp3"));
		Rolling.rollingTeste("Testando".getBytes());
		Rolling.rollingTeste("estando!".getBytes());
		Rolling.rollingTeste("stando ".getBytes());
	}
	
}
