package deduplication;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

import me.prettyprint.hector.api.beans.HColumn;

import org.apache.cassandra.thrift.Cassandra.system_add_column_family_args;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.Logger;

import deduplication.checksum.rsync.Checksum32;
import deduplication.dao.ChunksDao;
import deduplication.dao.operation.ChunksDaoOperations;
import deduplication.delta.Chunk;
import deduplication.gui.MainGUI;
import deduplication.processing.EagleEye;
import deduplication.processing.RollingInBruteForce;
import deduplication.processing.file.Chunking;
import deduplication.utils.FileUtils;

public class Main {
	
	private static final Logger log = Logger.getLogger(Main.class);
	
	private static String defaultPartition = "E"; 
	private static int defaultChunkSize = 4;
	
	private static File file;
	private static File modifiedFile;
	
	private static String fileName = "lorem.txt";
	private static String modifiedFileName = "lorem_modified.txt";
	
	public static void main (String[] args) {		
		file = new File(defaultPartition + ":\\teste\\" + fileName);
		modifiedFile = new File(defaultPartition + ":\\teste\\" + modifiedFileName);
		
		System.setProperty("defaultPartition", defaultPartition);
		
		//analysis_1();
		//analysis_2();
		//analysis_3();
		//analysis_4();
		//analysis_5();
		//analysis_6();
		//analysis_7();
		
		new MainGUI();
		
		/*ChunksDaoOperations cdo = new ChunksDaoOperations("TestCluster", "Dedupeer");
		System.out.println(cdo.getValuesWithContent("paulo", "lorem.txt").size());*/
		
	}
	
	private static void analysisBruteForce() {
		RollingInBruteForce.duplicationIdentification(FileUtils.getBytesFromFile("D:/dedup.txt"), "tando a".getBytes());
	}	
	
	/**
	 * Quebra um arquivo e chunks e verifica se todos esses chunks estão no mesmo arquivo. O número de chunks encontrados
	 * deve ser igual ao de chunks divididos inicialmente no método.
	 */
	private static void analysis_1() {
		try { Chunking.slicingAndDicing(file, new String(defaultPartition + ":\\teste\\chunks\\"), defaultChunkSize, String.valueOf(System.currentTimeMillis()), null); 
		} catch (IOException e) { e.printStackTrace(); }		
		
		byte[] flac = FileUtils.getBytesFromFile(file.getAbsolutePath());		
		byte[] chunk = FileUtils.getBytesFromFile((new File(defaultPartition + ":\\teste\\chunks\\" + FileUtils.getOnlyName(file.getName()) + "_chunk.0")).getAbsolutePath());
		
		ArrayList<Integer> hashes = Chunking.computeHashes(defaultPartition + ":\\teste\\chunks\\", FileUtils.getOnlyName(file.getName()) + "_chunk");
				
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
				log.info("Achou " + hash + " [count = " + count + "] e [index = " + index + "]");
				index += chunk.length;
				count++;
				continue;
			}			
			
			index++;
		}
		
		log.info("Processed in " + (System.currentTimeMillis() - time) + " miliseconds");
	}
	
	/**
	 * Encontrar um chunk dentro de um arquivo original através do rolling checksum
	 */	
	private static void analysis_2(){
		int chunks = 0;
		try { 
			chunks = Chunking.slicingAndDicing(file, new String(defaultPartition + ":\\teste\\chunks\\"), defaultChunkSize, String.valueOf(System.currentTimeMillis()), null).size(); 
		} catch (IOException e) { e.printStackTrace(); }	
		
		byte[] flac = FileUtils.getBytesFromFile(file.getAbsolutePath());
		int random = (new Random()).nextInt(chunks - 1);
		byte[] chunk = FileUtils.getBytesFromFile((new File(defaultPartition + ":\\teste\\chunks\\" + FileUtils.getOnlyName(file.getName()) + "_chunk." + random)).getAbsolutePath());

		log.info("Searching for chunk " + random);
		EagleEye.searchDuplication(flac, chunk);
	}
	
	/**
	 * Quebra um arquivo modificado em pedaços e em seguida compara para identificar quando chunks são iguais aos do arquivo original
	 */
	private static void analysis_3() {
		try { Chunking.slicingAndDicing(modifiedFile, new String(defaultPartition + ":\\teste\\chunks\\"), defaultChunkSize, String.valueOf(System.currentTimeMillis()), null); 
		} catch (IOException e) { e.printStackTrace(); }

		long time = System.currentTimeMillis();
		
		File file = new File(defaultPartition + ":/teste/matchless.flac");
		byte[] flac = FileUtils.getBytesFromFile(file.getAbsolutePath());
				
		ArrayList<Integer> hashes = Chunking.computeHashes(defaultPartition + ":\\teste\\chunks\\", FileUtils.getOnlyName(modifiedFile.getName()) + "_chunk");
		
		int count = 0;
		int lastIndex = 0;
		for(int hash: hashes) {
			int index = EagleEye.searchDuplication(flac, hash, lastIndex, defaultChunkSize);
			if(index != -1) {
				count++;
				lastIndex = index;
			}
		}
		
		log.info("Processed in " + (System.currentTimeMillis() - time) + " miliseconds");
		log.info(count + " chunks found!");

	}
	
	/**
	 * Criação de uma lista de reconstrução e utilização da mesma para a obtenção de um arquivo com uma 
	 * parte adicionado a partir dos chunks do antigo.
	 */
	public static void analysis_4() {
		HashMap<Integer, Chunk> rebuild = new HashMap<Integer, Chunk>();
		try { 
			Chunking.slicingAndDicing(file, new String(defaultPartition + ":\\teste\\chunks\\"), defaultChunkSize, String.valueOf(System.currentTimeMillis()), null); 
		} catch (IOException e) { 
			e.printStackTrace(); 
		}
		
		String path = defaultPartition + ":\\teste\\chunks\\";
		String initalNameOfChunk = FileUtils.getOnlyName(file.getName()) + "_chunk";
		
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
			
			log.info("\nSearching for " + c);
			
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
		
		log.info("\nProcessed in " + (System.currentTimeMillis() - time) + " miliseconds\n");		
		
		log.info("Rebuild:");
		for(Chunk c: rebuild.values()) {
			log.info(c.getOffset() + " -> " + (c.getOffset() + c.getLength()) + " with index in remote file = " + c.getIndexInRemoteFile());
		}
		
		log.info("\nRebuilding the new file with chunks of the old file...");
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
	
	/**
	 * Quebra um arquivo e salva as informações e o conteúdo (em bytes) de chunk no Cassandra
	 */
	public static void analysis_5() {		
		long time = System.currentTimeMillis();
		ArrayList<ChunksDao> chunks = new ArrayList<ChunksDao>();
		try { 
			chunks = Chunking.slicingAndDicing(file, new String(defaultPartition + ":\\teste\\chunks\\"), defaultChunkSize, String.valueOf(System.currentTimeMillis()), null); 
		} catch (IOException e) { 
			e.printStackTrace(); 
		}
		
		ChunksDaoOperations cdo = new ChunksDaoOperations("TestCluster", "Dedupeer");
		cdo.insertRows(chunks);
		
		log.info("Processed in " + (System.currentTimeMillis() - time) + " miliseconds");
	}
	
	/**
	 * Criação de uma lista de reconstrução e utilização da mesma para a obtenção de um arquivo com uma 
	 * parte adicionado a partir dos chunks do antigo, usando os dados no Cassandra.
	 */
	public static void analysis_6() {
		long time = System.currentTimeMillis();
		
		HashMap<Integer, Chunk> rebuild = new HashMap<Integer, Chunk>();
		
		ArrayList<ChunksDao> chunks = new ArrayList<ChunksDao>();
		try { 
			chunks = Chunking.slicingAndDicing(file, new String(defaultPartition + ":\\teste\\chunks\\"), defaultChunkSize, String.valueOf(System.currentTimeMillis()), null); 
		} catch (IOException e) { 
			e.printStackTrace(); 
		}
		
		ChunksDaoOperations cdo = new ChunksDaoOperations("TestCluster", "Dedupeer");		
		cdo.insertRows(chunks);
				
		int amountChunk = chunks.size();
		
		byte[] modFile = FileUtils.getBytesFromFile(modifiedFile.getAbsolutePath());
		
		Checksum32 c32 = new Checksum32();
		int lastIndex = 0;
		
		for(int i = 0; i < amountChunk; i++) {
			HColumn<String, String> columnAdler32 = cdo.getValues(chunks.get(i).fileID, String.valueOf(i)).get().getColumns().get(0);
			HColumn<String, String> columnContent = cdo.getValues(chunks.get(i).fileID, String.valueOf(i)).get().getColumns().get(1);
			HColumn<String, String> columnIndex = cdo.getValues(chunks.get(i).fileID, String.valueOf(i)).get().getColumns().get(2);
			HColumn<String, String> columnLength = cdo.getValues(chunks.get(i).fileID, String.valueOf(i)).get().getColumns().get(3);
			HColumn<String, String> columnMd5 = cdo.getValues(chunks.get(i).fileID, String.valueOf(i)).get().getColumns().get(4);
			
			byte[] chunk = columnContent.getValue().getBytes();
			
			c32.check(chunk, 0, chunk.length);
			int index = EagleEye.searchDuplication(modFile, c32.getValue(), lastIndex, chunk.length);
			if(index != -1) {
				rebuild.put(index, new Chunk(index, chunk.length, 
						(i * chunk.length)//position in the remote file TODO put this data in a database
						));
				lastIndex = index;
			}
		}
				
		log.info("\nProcessed in " + (System.currentTimeMillis() - time) + " miliseconds\n");		
				
		log.info("\nRebuilding the new file with chunks of the old file...");
		byte[] rebuildFile = new byte[modFile.length];		
		for(int j = 0; j < rebuildFile.length;) {
			if(rebuild.containsKey(j)) {
				byte[] oldChunk = cdo.getValues(chunks.get(0).fileID, String.valueOf((rebuild.get(j).getIndexInRemoteFile()/defaultChunkSize))).get().getColumns().get(1).getValue().getBytes();
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
		
		log.info("Processed in " + (System.currentTimeMillis() - time) + " miliseconds");
	}
	
	/**
	 * Salva um arquivo no cassandra, em seguida pega o mesmo arquivo modificado, compara com o salvo no cassandra, indentifica
	 * os chunks duplicados e salva o arquivo no cassandra com a referencia para os chunks duplicados com o primeiro arquivo
	 */
	public static void analysis_7() {
		long time = System.currentTimeMillis();
				
		ArrayList<ChunksDao> chunks = new ArrayList<ChunksDao>();
		try { 
			chunks = Chunking.slicingAndDicing(file, new String(defaultPartition + ":\\teste\\chunks\\"), defaultChunkSize, String.valueOf(System.currentTimeMillis()), null); 
		} catch (IOException e) { 
			e.printStackTrace(); 
		}
		
		ChunksDaoOperations cdo = new ChunksDaoOperations("TestCluster", "Dedupeer");		
		cdo.insertRows(chunks);
				
		int amountChunk = chunks.size();
		
		byte[] modFile = FileUtils.getBytesFromFile(modifiedFile.getAbsolutePath());
		HashMap<Integer, ChunksDao> newFileChunks = new HashMap<Integer, ChunksDao>();
		long modFileID = System.currentTimeMillis();
		int chunk_number = 0;
		
		Checksum32 c32 = new Checksum32();
		int lastIndex = 0;
		
		//encontra os chunk duplicados no sistema
		for(int i = 0; i < amountChunk; i++) {
			HColumn<String, String> columnAdler32 = cdo.getValues(chunks.get(i).fileID, String.valueOf(i)).get().getColumns().get(0);
			HColumn<String, String> columnContent = cdo.getValues(chunks.get(i).fileID, String.valueOf(i)).get().getColumns().get(1);
			HColumn<String, String> columnLength = cdo.getValues(chunks.get(i).fileID, String.valueOf(i)).get().getColumns().get(3);
			
			//TODO comparar também o md5 quando achar um adler32 no arquivo modificado
			HColumn<String, String> columnMd5 = cdo.getValues(chunks.get(i).fileID, String.valueOf(i)).get().getColumns().get(4);
			
			byte[] chunk = columnContent.getValue().getBytes();
			
			int index = EagleEye.searchDuplication(modFile, Integer.parseInt(columnAdler32.getValue()), lastIndex, chunk.length);
			if(index != -1) {
				newFileChunks.put(index, new ChunksDao(String.valueOf(modFileID), String.valueOf(chunk_number++), String.valueOf(index), columnLength.getValue(),
						chunks.get(i).fileID, chunks.get(i).chunkNumber));
				lastIndex = index;
			}
		}
		
		int index = 0;
		ByteBuffer buffer = ByteBuffer.allocate(defaultChunkSize);
		while(index < modFile.length) {
			if(newFileChunks.containsKey(index)) {
				if(buffer.position() > 0) { //se o buffer ja tem alguns dados, cria um chunk com ele
					byte[] newchunk = Arrays.copyOfRange(buffer.array(), 0, buffer.position());
					c32.check(newchunk, 0, newchunk.length);
					newFileChunks.put(index - buffer.position(), new ChunksDao(String.valueOf(modFileID), String.valueOf(chunk_number), 
							DigestUtils.md5Hex(newchunk), String.valueOf(c32.getValue()), String.valueOf(index - buffer.position()), 
							String.valueOf(buffer.capacity()), newchunk));
					
					buffer.clear();
				}
				index += Integer.parseInt(newFileChunks.get(index).length); //pula, porque o chunk já foi inserido na comparação com o outro arquivo
			} else {
				if(buffer.remaining() == 0) {
					c32.check(buffer.array(), 0, buffer.capacity());
					newFileChunks.put(index - buffer.capacity(), new ChunksDao(String.valueOf(modFileID), String.valueOf(chunk_number), 
							DigestUtils.md5Hex(buffer.array()), String.valueOf(c32.getValue()), String.valueOf(index - buffer.capacity()), 
							String.valueOf(buffer.capacity()), buffer.array()));
					chunk_number++;
					
					buffer.clear();
				} else {
					buffer.put(modFile[index]);
					index++;
				}
			}
		}
		if(buffer.position() > 0) { //se o buffer ja tem alguns dados, cria um chunk com ele
			chunk_number++;
			newFileChunks.put(index - buffer.position(), new ChunksDao(String.valueOf(modFileID), String.valueOf(chunk_number), 
					DigestUtils.md5Hex(Arrays.copyOfRange(buffer.array(), 0, buffer.position())), String.valueOf(c32.getValue()), String.valueOf(index - buffer.position()), 
					String.valueOf(buffer.capacity()), Arrays.copyOfRange(buffer.array(), 0, buffer.position())));
			
			buffer.clear();
		}
		
		for(ChunksDao chunk: newFileChunks.values()) {
			cdo.insertRow(chunk);			
		}
				
		log.info("Processed in " + (System.currentTimeMillis() - time) + " miliseconds");
		
		for(int i = 0; i < newFileChunks.size(); i++) {
			System.out.println("chunk: " + cdo.getValues(newFileChunks.get(0).fileID, String.valueOf(i)).get());
		}
	}
	
	
}
