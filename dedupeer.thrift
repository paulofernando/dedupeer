namespace java com.dedupeer.thrift

typedef i32 int
typedef i32 weakHash

typedef i64 position

typedef string strongHash
typedef string chunkID

typedef map<weakHash,map<strongHash,chunkID>> hashesToCompare

struct Chunk {
	1:string fileID,
	2:string chunkNumber,
	3:string md5,
	4:string adler32,
	5:string index,
	6:string length,
	7:string pfile,
	8:string pchunk, 
	9:string destination
	//10:binary content
}

service DeduplicationService {	
	map<position,Chunk> deduplicate(1:hashesToCompare chunksInfo, 2:string pathOfFile, 3:int chunkSizeInBytes, 4:int bytesToLoadByTime),
}
