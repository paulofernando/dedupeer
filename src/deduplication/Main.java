package deduplication;

import java.util.HashSet;

import deduplication.checksum.Hashing;
import deduplication.checksum.RabinKarp;
import deduplication.checksum.RollingAlder32;
import deduplication.checksum.RollingChecksum;
import deduplication.processing.EagleEye;
import deduplication.utils.FileUtils;

public class Main {
	
	public static void main (String[] args) {
		//System.out.println(Hashing.getSHA1("Testando!".getBytes()));
		//RollingAlder32.rollingIn("Testando a parada aqui pra ver se está na paz de Jah".getBytes(), 0, 10);
		//EagleEye.duplicationIdentification(FileUtils.getBytesFromFile("D:/dedup.txt"), "tando a".getBytes());
		//EagleEye.duplicationIdentification(FileUtils.getBytesFromFile("D:/teste/teddy_picker_5s.mp3"), FileUtils.getBytesFromFile("D:/teste/teddy_picker_chunk.mp3"));
		/*Rolling.rollingTeste("Testando".getBytes());
		Rolling.rollingTeste("estando!".getBytes());
		Rolling.rollingTeste("stando ".getBytes());*/
		
		//-------------------------------------------------------------------------------------------------------------
		/*byte[] txt = FileUtils.getBytesFromFile("D:/teste/lorem.txt");
		byte[] pat = FileUtils.getBytesFromFile("D:/teste/chunk.txt");		
		
		long time = System.currentTimeMillis();		
		EagleEye.duplicationIdentification(txt,  pat);
		System.out.println("Básico executado em " + (System.currentTimeMillis() - time) + "ms");
				
		time = System.currentTimeMillis();
		RabinKarp searcher = new RabinKarp(new String(pat));
        int offset = searcher.search(new String(txt));        
        
        System.out.println("Offset: " + offset);
        System.out.println("Rabin-Karp executado em " + (System.currentTimeMillis() - time) + "ms");*/
        //-------------------------------------------------------------------------------------------------------------
        
		byte[] txt = FileUtils.getBytesFromFile("D:/teste/lorem.txt");
		Long hash = RollingChecksum.sum(txt);
		//HashSet<Long> hashes = new HashSet<Long>();
		RollingChecksum checksum = new RollingChecksum(txt, 3000);
		
		int i = 0;
		while (checksum.next()) {
			long cs = checksum.weak();
			if(cs == hash) {
				System.out.println("Achou!");
			}			
			System.out.println(cs);
			i++;
		}
		
		System.out.println(hash);
        
	}
	
}
