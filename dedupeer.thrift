namespace java com.dedupeer.thrift

typedef i32 int
typedef i32 weakHash

typedef i64 long
typedef i64 position

typedef string strongHash
typedef string chunkID

typedef map<weakHash,map<strongHash,chunkID>> chunksToCompare

service DeduplicationService {	
	map<position,chunkID> deduplicate(1:chunksToCompare chunks, 2:string pathOfFile),
}
