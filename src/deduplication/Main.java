package deduplication;

import java.io.File;
import java.io.IOException;

import deduplication.processing.file.Chunking;

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
        
		/*byte[] txt = FileUtils.getBytesFromFile("D:/teste/LH_30s.mp3");
		byte[] chunk = FileUtils.getBytesFromFile("D:/teste/LH_chunk.mp3");
		Long hash = RollingChecksum.sum(chunk);
		RollingChecksum checksum = new RollingChecksum(txt, chunk.length);
		
		int i = 0;
		while (checksum.next()) {
			long cs = checksum.weak();
			
			if(cs == hash) {				
				System.out.println("************************************* Achou! [index = " + i +"]");
				System.out.println(cs);
			}
			i++;
		}
		*/
		//-------------------------------------------------------------------------------------------------------------
		
		/*try {
			Chunking.slicingAndDicing(new File("D:/teste/matchless.flac"), new String("D:\\teste\\chunks\\"), 16000);
		} catch (IOException e) {
			e.printStackTrace();
		}*/
		
		
		//-------------------------------------------------------------------------------------------------------------
		
		Chunking.restoreFile("D:\\teste\\chunks\\", "chunk");
		
	}
	
}
