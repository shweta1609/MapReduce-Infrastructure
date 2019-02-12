#pragma once

#include <vector>
#include "mapreduce_spec.h"
#include <math.h>
#include <sstream>

using namespace std;

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {
vector<string> files;
vector<int> offset_start;
vector<int> offset_end;
};


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
	double total_size =0;
	vector<int> charcount;
	for(int i=0;i<mr_spec.input_files.size();i++)
	{
		ifstream file(mr_spec.input_files[i], ifstream::ate | ifstream::binary);
		//cout<<mr_spec.input_files[i]<<endl;
		//cout<<file.tellg()<<endl;
		charcount.push_back(file.tellg());
		total_size += (charcount[i]/1024);
	}
	int shards = ceil(total_size/mr_spec.map_kilobytes);

	int shard_no =0;
	FileShard fs;
	for(int i=0;i<shards;i++) {
		fileShards.push_back(fs);
	}
	for(int i =0 ;i< mr_spec.input_files.size();i++)
	{
		int shard_no_count=0;
		//while(shard_no_count<mr_spec.map_kilobytes*1024) {

		//shard_no_count=0;
		fileShards[shard_no].files.push_back(mr_spec.input_files[i]);
		fileShards[shard_no].offset_start.push_back(shard_no_count);

		ifstream ifile;
		ifile.open(mr_spec.input_files[i]);
		string output;
		if (ifile.is_open()) {
			int charc=0;
			while (getline(ifile, output)) {

				if(shard_no_count>mr_spec.map_kilobytes*1024)
				{
					fileShards[shard_no].offset_end.push_back(charc);
					shard_no++;

					if(shard_no<shards) {
						fileShards[shard_no].files.push_back(mr_spec.input_files[i]);
						fileShards[shard_no].offset_start.push_back(charc + 1);

					}
					shard_no_count=0;
				}
				charc+=output.size();
				shard_no_count += output.size();
			}

			if(shard_no_count>mr_spec.map_kilobytes*1024)
			{
				shard_no++;
			}
			fileShards[shard_no].offset_end.push_back(charc);
		}

		//}
	}
	return true;
}
