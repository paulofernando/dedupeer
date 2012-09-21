package deduplication;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import deduplication.checksum.rsync.Checksum32;
import deduplication.delta.Chunk;
import deduplication.processing.EagleEye;
import deduplication.processing.RollingInBruteForce;
import deduplication.processing.file.Chunking;
import deduplication.utils.FileUtils;

public class Main {
	
	private static String defaultPartition = "D"; 
	private static int chunkSize = 64000;
	
	private static File file;
	private static File modifiedFile;
	
	public static void main (String[] args) {		
		file = new File(defaultPartition + ":/teste/matchless.flac");
		modifiedFile = new File(defaultPartition + ":\\teste\\matchless_modified.flac");
		
		//analysis_1();
		//analysis_2();
		analysis_3();
		//analysis_4();
	}
	
	private static void analysisBruteForce() {
		RollingInBruteForce.duplicationIdentification(FileUtils.getBytesFromFile("D:/dedup.txt"), "tando a".getBytes());
	}
	
	/**
	 * Quebra um arquivo e chunks e verifica se todos esses chunks estão no mesmo arquivo. O número de chunks encontrados
	 * deve ser igual ao de chunks divididos inicialmente no método.
	 */
	private static void analysis_1() {
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
		try { Chunking.slicingAndDicing(modifiedFile, new String(defaultPartition + ":\\teste\\chunks\\"), chunkSize); 
		} catch (IOException e) { e.printStackTrace(); }

		long time = System.currentTimeMillis();
		
		File file = new File(defaultPartition + ":/teste/matchless.flac");
		byte[] flac = FileUtils.getBytesFromFile(file.getAbsolutePath());
				
		ArrayList<Integer> hashes = Chunking.computeHashes(defaultPartition + ":\\teste\\chunks\\", FileUtils.getOnlyName(modifiedFile) + "_chunk");
		
		int count = 0;
		for(int hash: hashes) {
			if(EagleEye.searchDuplication(flac, hash, 0, chunkSize) != -1) {
				count++;
			}
		}
		
		System.out.println("Processed in " + (System.currentTimeMillis() - time) + " miliseconds");
		System.out.println(count + " chunks found!");

	}
	
	/**
	 * Criação de uma lista de reconstrução e utilização da mesma para a obtenção de um arquivo com uma 
	 * parte adicionado a partir dos chunks do antigo.
	 */
	public static void analysis_4() {
		ArrayList<Chunk> rebuild = new ArrayList<Chunk>();
		try { Chunking.slicingAndDicing(file, new String(defaultPartition + ":\\teste\\chunks\\"), chunkSize); 
		} catch (IOException e) { e.printStackTrace(); }
				
		long time = System.currentTimeMillis();
		
	}
	
}
