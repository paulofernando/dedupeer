package deduplication;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import deduplication.checksum.rsync.Checksum32;
import deduplication.processing.EagleEye;
import deduplication.processing.RollingInBruteForce;
import deduplication.processing.file.Chunking;
import deduplication.utils.FileUtils;

public class Main {
	
	private static String defaultPartition = "D"; 
	
	public static void main (String[] args) {
		//System.out.println(Hashing.getSHA1("Testando!".getBytes()));
		//RollingAlder32.rollingIn("Testando a parada aqui pra ver se está na paz de Jah".getBytes(), 0, 10);
		//
		//RollingInBruteForce.duplicationIdentification(FileUtils.getBytesFromFile("D:/teste/teddy_picker_5s.mp3"), FileUtils.getBytesFromFile("D:/teste/teddy_picker_chunk.mp3"));
		/*Rolling.rollingTeste("Testando".getBytes());
		Rolling.rollingTeste("estando!".getBytes());
		Rolling.rollingTeste("stando ".getBytes());*/
		
		//-------------------------------------------------------------------------------------------------------------
		/*byte[] txt = FileUtils.getBytesFromFile("D:/teste/lorem.txt");
		byte[] pat = FileUtils.getBytesFromFile("D:/teste/chunk.txt");		
		
		long time = System.currentTimeMillis();		
		RollingInBruteForce.duplicationIdentification(txt,  pat);
		System.out.println("Básico executado em " + (System.currentTimeMillis() - time) + "ms");
				
		time = System.currentTimeMillis();
		RabinKarp searcher = new RabinKarp(new String(pat));
        int offset = searcher.search(new String(txt));        
        
        System.out.println("Offset: " + offset);
        System.out.println("Rabin-Karp executado em " + (System.currentTimeMillis() - time) + "ms");*/
        //-------------------------------------------------------------------------------------------------------------
        
		/*byte[] txt = FileUtils.getBytesFromFile("D:/teste/LH_30s.mp3");
		byte[] chunk = FileUtils.getBytesFromFile("D:/teste/LH_chunk.mp3");
		Long hash = RollingChecksumOlder.sum(chunk);
		RollingChecksumOlder checksum = new RollingChecksumOlder(txt, chunk.length);
		
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
				
		/*File file = new File("E:/teste/matchless.flac");
		try {			
			Chunking.slicingAndDicing(file, new String("E:\\teste\\chunks\\"), 16000);
		} catch (IOException e) {
			e.printStackTrace();
		}
		Chunking.restoreFile("E:\\teste\\chunks\\", FileUtils.getOnlyName(file) + "_chunk", "E:\\teste\\restored\\restored.flac");
		
		byte[] flac = FileUtils.getBytesFromFile("E:\\teste\\restored\\restored.flac");
		byte[] chunk = FileUtils.getBytesFromFile("E:\\teste\\matchless_chunk.flac");
		
		EagleEye.searchDuplication(flac, chunk);*/
		
		//-------------------------------------------------------------------------------------------------------------
		/*File file = new File("E:/teste/matchless.flac");
		ArrayList<Long> hashes = Chunking.computeHashes("E:\\teste\\chunks\\", FileUtils.getOnlyName(file) + "_chunk");
		
		byte[] flac = FileUtils.getBytesFromFile(file.getAbsolutePath());
		long sizeOfChunk = (new File("E:\\teste\\chunks\\" + FileUtils.getOnlyName(file) + "_chunk.1")).length();
		
		long time = System.currentTimeMillis();
		
		int index = 0;
		int count = 0;
		for(Long hash: hashes) {
			index = EagleEye.searchDuplicationWithoutRollingChecksum(flac, index, hash, (int)sizeOfChunk);
			if(index != -1) {
				System.out.println("Achou: [index: " + index + "]");
				count++;
			} else {
				System.out.println("Não achou o chunk com hash: " + hash);
			}
		}
		
		System.out.println("\nFile " + ((count * 100)/hashes.size()) + "% duplicated\nProcess finished in " + 
				((System.currentTimeMillis() - time)/1000) + " seconds");*/
		
		//----------------------------------------------------------------------------------------------------------------
		
		//analysis_2();
		analysis_3();
	}
	
	private static void analysisBruteForce() {
		RollingInBruteForce.duplicationIdentification(FileUtils.getBytesFromFile("D:/dedup.txt"), "tando a".getBytes());
	}
	
	/**
	 * Quebra um arquivo e chunks e verifica se todos esses chunks estão no mesmo arquivo. O número de chunks encontrados
	 * deve ser igual ao de chunks divididos inicialmente no método.
	 */
	private static void analysis_1() {
		File file = new File(defaultPartition + ":/teste/matchless.flac");		
		
		try { Chunking.slicingAndDicing(file, new String(defaultPartition + ":\\teste\\chunks\\"), 64000); 
		} catch (IOException e) { e.printStackTrace(); }		
		
		byte[] flac = FileUtils.getBytesFromFile(file.getAbsolutePath());		
		byte[] chunk = FileUtils.getBytesFromFile((new File(defaultPartition + ":\\teste\\chunks\\" + FileUtils.getOnlyName(file) + "_chunk.0")).getAbsolutePath());
		
		ArrayList<Integer> hashes = Chunking.computeHashes(defaultPartition + ":\\teste\\chunks\\", FileUtils.getOnlyName(file) + "_chunk");
				
		long time = System.currentTimeMillis();		
		Checksum32 c32 = new Checksum32();		
		Integer hash;;
		int index = 0;
		int count = 0;
		while(index < flac.length - chunk.length) {
			
			if(index % chunk.length == 0) {
				c32.check(flac, index, chunk.length);
			} else {			
				c32.roll((byte)1);
			}
			
			hash = c32.getValue();
			if(hashes.contains(hash)) {
				System.out.println("Achou " + hash + " [count = " + count + "] e [index = " + index + "]");
				index += chunk.length;
				count++;
				continue;
			}			
			
			index++;
		}
		
		System.out.println("Processed in " + (System.currentTimeMillis() - time) + " miliseconds");
	}
	
	/**
	 * Encontrar um chunk dentro de um arquivo original através do rolling checksum
	 */	
	private static void analysis_2(){
		File file = new File(defaultPartition + ":/teste/matchless.flac");		
		
		int chunks = 0;
		try { 
			chunks = Chunking.slicingAndDicing(file, new String(defaultPartition + ":\\teste\\chunks\\"), 64000); 
		} catch (IOException e) { e.printStackTrace(); }	
		
		byte[] flac = FileUtils.getBytesFromFile(file.getAbsolutePath());
		int random = (new Random()).nextInt(chunks - 1);
		byte[] chunk = FileUtils.getBytesFromFile((new File(defaultPartition + ":\\teste\\chunks\\" + FileUtils.getOnlyName(file) + "_chunk." + random)).getAbsolutePath());

		System.out.println("Searching for chunk " + random);
		EagleEye.searchDuplication(flac, chunk);
	}
	
	/**
	 * Quebra umarquivo modificado em pedaços e em seguida compara para identificar quando chunks são iguais aos do arquivo original
	 */
	private static void analysis_3() {
		File newFile = new File(defaultPartition + ":\\teste\\matchless_modified.flac");
		
		try { Chunking.slicingAndDicing(newFile, new String(defaultPartition + ":\\teste\\chunks\\"), 64000); 
		} catch (IOException e) { e.printStackTrace(); }

		long time = System.currentTimeMillis();
		
		File file = new File(defaultPartition + ":/teste/matchless.flac");
		byte[] flac = FileUtils.getBytesFromFile(file.getAbsolutePath());
		int chunkLength = FileUtils.getBytesFromFile((new File(defaultPartition + ":\\teste\\chunks\\" + FileUtils.getOnlyName(newFile) + "_chunk.0")).getAbsolutePath()).length;
		
		ArrayList<Integer> hashes = Chunking.computeHashes(defaultPartition + ":\\teste\\chunks\\", FileUtils.getOnlyName(newFile) + "_chunk");
		
		int count = 0;
		for(int hash: hashes) {
			if(EagleEye.searchDuplication(flac, hash, 0, chunkLength) != -1) {
				count++;
			}
		}
		
		System.out.println("Processed in " + (System.currentTimeMillis() - time) + " miliseconds");
		System.out.println(count + " chunks found!");

	}
	
	/**
	 * Criação de uma lista de reconstrução e utilização da meemsa para a obtenção de um arquivo com uma 
	 * parte adicionado a partir dos chunks do antigo.
	 */
	public static void analysis_4() {
		
	}
	
}
