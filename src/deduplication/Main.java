package deduplication;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

import deduplication.checksum.rsync.Checksum32;
import deduplication.delta.Chunk;
import deduplication.processing.EagleEye;
import deduplication.processing.RollingInBruteForce;
import deduplication.processing.file.Chunking;
import deduplication.utils.FileUtils;

public class Main {
	
	private static String defaultPartition = "D"; 
	private static int defaultChunkSize = 4;
	
	private static File file;
	private static File modifiedFile;
	
	private static String fileName = "lorem.txt";
	private static String modifiedFileName = "lorem_modified.txt";
	
	public static void main (String[] args) {		
		file = new File(defaultPartition + ":\\teste\\" + fileName);
		modifiedFile = new File(defaultPartition + ":\\teste\\" + modifiedFileName);
		
		//test1();
		//test2();
		
		//analysis_1();
		//analysis_2();
		//analysis_3();
		analysis_4();
	}
	
	private static void analysisBruteForce() {
		RollingInBruteForce.duplicationIdentification(FileUtils.getBytesFromFile("D:/dedup.txt"), "tando a".getBytes());
	}
	
	
	/**
	 * Teste para idetificar se o algoritmo de quebra de chunks estão quebrando o arquivo de forma correta.
	 */
	private static void test1() {
		try { 
			Chunking.slicingAndDicing(file, new String(defaultPartition + ":\\teste\\chunks\\"), defaultChunkSize); 
		} catch (IOException e) { 
			e.printStackTrace(); 
		}
		
		byte[] chunk0 = FileUtils.getBytesFromFile((new File(defaultPartition + ":\\teste\\chunks\\" + FileUtils.getOnlyName(file) + "_chunk.0")).getAbsolutePath());		
		byte[] chunk1 = FileUtils.getBytesFromFile((new File(defaultPartition + ":\\teste\\chunks\\" + FileUtils.getOnlyName(file) + "_chunk.1")).getAbsolutePath());
		byte[] chunk2 = FileUtils.getBytesFromFile((new File(defaultPartition + ":\\teste\\chunks\\" + FileUtils.getOnlyName(file) + "_chunk.2")).getAbsolutePath());
		
		if((chunk0[chunk0.length - 1] == chunk1[0]) && (chunk1[chunk1.length - 1] == chunk2[0])) {
			System.out.println("Probably buggy");
		} else {
			System.out.println("Jah reigns!\nchunk0[" + (chunk0.length - 1) + "] == " + chunk0[chunk0.length - 1] + " != " + " chunk1[0] == " + chunk1[0]);
			System.out.println("chunk1[" + (chunk1.length - 1) + "] == " + chunk1[chunk1.length - 1] + " != " + " chunk2[0] == " + chunk2[0]);
		}
	}
	
	/**
	 * Teste para verificar se o arquivo estão sendo divido corretamente.
	 */	
	private static void test2() {
		defaultChunkSize = 4;
		File txtFile = new File(defaultPartition + ":/teste/lorem.txt");
		
		try { Chunking.slicingAndDicing(txtFile, new String(defaultPartition + ":\\teste\\chunks\\"), defaultChunkSize); 
		} catch (IOException e) { e.printStackTrace(); }
		
		String path = defaultPartition + ":\\teste\\chunks\\";
		String initalNameOfCHunk = FileUtils.getOnlyName(txtFile) + "_chunk";
				
		byte[] txtFileBytes = FileUtils.getBytesFromFile(txtFile.getAbsolutePath());
		
		int currentChunk = 0;
		boolean equals = true;
		
		int lastChunkSize = (int)txtFile.length() % defaultChunkSize;
		long totalChunks = (long)Math.ceil((double)txtFile.length()/(double)defaultChunkSize);
		
		while(currentChunk < totalChunks) {
			byte[] chunk = FileUtils.getBytesFromFile(path + initalNameOfCHunk + "." + currentChunk);
						
			String dividedChunk = new String(chunk);
			String originalChunk = new String(Arrays.copyOfRange(txtFileBytes, currentChunk * defaultChunkSize, 
					(currentChunk * defaultChunkSize) + (currentChunk == (totalChunks - 1) ? lastChunkSize : defaultChunkSize)));
			
			if(!dividedChunk.equals(originalChunk))  {
				equals = false;
				System.out.println(dividedChunk + " != " + originalChunk);
			} else {
				System.out.println(dividedChunk + " == " + originalChunk);
			}
			currentChunk++;
		}
		
		if(!equals) {
			System.out.println("Falha na divisão");
		} else {
			System.out.println("Divisão correta");
		}
		
		defaultChunkSize = 64000;
	}
	
	/**
	 * Quebra um arquivo e chunks e verifica se todos esses chunks estï¿½o no mesmo arquivo. O número de chunks encontrados
	 * deve ser igual ao de chunks divididos inicialmente no método.
	 */
	private static void analysis_1() {
		try { Chunking.slicingAndDicing(file, new String(defaultPartition + ":\\teste\\chunks\\"), defaultChunkSize); 
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
			chunks = Chunking.slicingAndDicing(file, new String(defaultPartition + ":\\teste\\chunks\\"), defaultChunkSize); 
		} catch (IOException e) { e.printStackTrace(); }	
		
		byte[] flac = FileUtils.getBytesFromFile(file.getAbsolutePath());
		int random = (new Random()).nextInt(chunks - 1);
		byte[] chunk = FileUtils.getBytesFromFile((new File(defaultPartition + ":\\teste\\chunks\\" + FileUtils.getOnlyName(file) + "_chunk." + random)).getAbsolutePath());

		System.out.println("Searching for chunk " + random);
		EagleEye.searchDuplication(flac, chunk);
	}
	
	/**
	 * Quebra um arquivo modificado em pedaços e em seguida compara para identificar quando chunks são iguais aos do arquivo original
	 */
	private static void analysis_3() {
		try { Chunking.slicingAndDicing(modifiedFile, new String(defaultPartition + ":\\teste\\chunks\\"), defaultChunkSize); 
		} catch (IOException e) { e.printStackTrace(); }

		long time = System.currentTimeMillis();
		
		File file = new File(defaultPartition + ":/teste/matchless.flac");
		byte[] flac = FileUtils.getBytesFromFile(file.getAbsolutePath());
				
		ArrayList<Integer> hashes = Chunking.computeHashes(defaultPartition + ":\\teste\\chunks\\", FileUtils.getOnlyName(modifiedFile) + "_chunk");
		
		int count = 0;
		int lastIndex = 0;
		for(int hash: hashes) {
			int index = EagleEye.searchDuplication(flac, hash, lastIndex, defaultChunkSize);
			if(index != -1) {
				count++;
				lastIndex = index;
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
		HashMap<Integer, Chunk> rebuild = new HashMap<Integer, Chunk>();
		try { 
			Chunking.slicingAndDicing(file, new String(defaultPartition + ":\\teste\\chunks\\"), defaultChunkSize); 
		} catch (IOException e) { 
			e.printStackTrace(); 
		}
		
		String path = defaultPartition + ":\\teste\\chunks\\";
		String initalNameOfChunk = FileUtils.getOnlyName(file) + "_chunk";
		
		long time = System.currentTimeMillis();
		
		byte[] modFile = FileUtils.getBytesFromFile(modifiedFile.getAbsolutePath());
		
		Checksum32 c32 = new Checksum32();
		int i = 0;
		int lastIndex = 0;
		while((new File(path + initalNameOfChunk + "." + i)).exists()) {
			byte[] chunk = FileUtils.getBytesFromFile(path + initalNameOfChunk + "." + i);
			
			String c = "[ ";
			for(byte b: chunk) {
				c += b + ", ";
			}
			c = c.substring(0, c.length() - 2) + "]";
			
			System.out.println("\nSearching for " + c);
			
			c32.check(chunk, 0, chunk.length);
			int index = EagleEye.searchDuplication(modFile, c32.getValue(), lastIndex, chunk.length);
			if(index != -1) {
				rebuild.put(index, new Chunk(index, chunk.length, 
						(i * chunk.length)//position in the remote file TODO put this data in a database
						));
				lastIndex = index;
			}
			i++;
		}
		
		System.out.println("\nProcessed in " + (System.currentTimeMillis() - time) + " miliseconds\n");		
		
		System.out.println("Rebuild:");
		for(Chunk c: rebuild.values()) {
			System.out.println(c.getOffset() + " -> " + (c.getOffset() + c.getLength()) + " with index in remote file = " + c.getIndexInRemoteFile());
		}
		
		System.out.println("\nRebuilding the new file with chunks of the old file...");
		byte[] rebuildFile = new byte[modFile.length];		
		for(int j = 0; j < rebuildFile.length;) {
			if(rebuild.containsKey(j)) {
				byte[] oldChunk = FileUtils.getBytesFromFile(path + initalNameOfChunk + "." + (rebuild.get(j).getIndexInRemoteFile()/defaultChunkSize));
				for(int b = j; b - j < oldChunk.length; b++) {
					rebuildFile[b] = oldChunk[b - j];
				}
				j += rebuild.get(j).getLength();
			} else {
				rebuildFile[j] = modFile[j];
				j++;
			}
		}
		
		System.out.print("[");
		for(byte b: rebuildFile) {
			System.out.print(b + " ");
		}
		System.out.print("]");
	}
	
}
