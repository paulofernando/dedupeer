namespace java com.dedupeer.thrift

typedef i32 int
typedef i32 weakHash

typedef i64 position

typedef string strongHash
typedef string chunkID

struct ChunkIDs {
	1: optional string fileID,
	2: required string chunkID,
}

typedef map<weakHash,map<strongHash,ChunkIDs>> hashesToCompare

struct Chunk {
	1: required string fileID,
	2: required string chunkNumber,
	3: required string md5,
	4: required string adler32,
	5: required string index,
	6: required string length,
	7: optional string pfile,
	8: optional string pchunk, 
	9: optional string destination
	10: optional binary content
}

service DeduplicationService {	
	map<position,Chunk> deduplicate(1:hashesToCompare chunksInfo, 2:string pathOfFile, 3:int chunkSizeInBytes, 4:int bytesToLoadByTime),
}
