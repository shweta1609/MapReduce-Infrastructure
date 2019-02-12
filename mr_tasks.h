#pragma once

#include <string>
#include <iostream>
#include <fstream>
#include <map>
#include <vector>
#include <bits/stdc++.h>
#include <stdio.h>

using namespace std;
/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

		/* DON'T change this function's signature */
		BaseMapperInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
        vector<map<string,string>> keyMap;
        int map_count;
        map<int,int> mapping;
		int num_outputs;
		string output_dirs;
		void init_params(int num_output){
			num_outputs = num_output;

			map_count =0;
		}

		void write_intoFile(string output_dirs)
        {
		    map<string,string>::iterator it;
            map<int,int>::iterator its;
		    for(its =mapping.begin();its!=mapping.end() ;its++)
            {
                int file_index = its->first;
                string file_name = output_dirs+"_"+to_string(file_index)+".txt";
                ofstream outputFile;
                //int a = remove(file_name);
                outputFile.open(file_name);
                if (outputFile.is_open()) {
                    for(it=keyMap[its->second].begin();it!=keyMap[its->second].end();it++)
                    {
                        outputFile<<it->first<<","<<it->second<<endl;
                    }
                } else{
                   // cout<<"nai na re bhava";
                }
                outputFile.close();
            }
        }
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
    //cout<<"key = "<<key<<" value = "<<val<<endl;
	//std::cout << "Dummy emit by BaseMapperInternal: " << key << ", " << val << std::endl;
	int file_index = hash<std::string>{}(key)%num_outputs;
	//string map_file = output_dirs+"map_file_"+to_string(file_index);
	map<int,int>::iterator it = mapping.find(file_index) ;
    if(it == mapping.end())
    {
        map<string, string> map_files;
        keyMap.push_back(map_files);
        mapping[file_index] = map_count;
        map_count++;
    }
    int req_index = mapping[file_index];

    map<string,string>::iterator its = keyMap[req_index].find(key);

   if(its == keyMap[req_index].end())
    {
        keyMap[req_index][key] = val;
        its =keyMap[req_index].find(key);
    }
   else if(its->first==key)
   {
        //cout<<" parat aali value-=============================  "<<its->second<<endl;
        its->second = to_string(stoi(its->second) + stoi(val));
       //cout<<" navin aali value-=============================  "<<its->second<<endl;

   }
   else
   {
   //    cout<<"Jhol ahe";
   }

}


/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

		/* DON'T change this function's signature */
		BaseReducerInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
        map<string,string> mapped;

    void write_intoFile(string shard_id)
    {
        map<string,string>::iterator it;

        string file_name = shard_id+".txt";
        ofstream outputFile;
        //int a = remove(file_name);
        outputFile.open(file_name);
        if (outputFile.is_open()) {
            for(it=mapped.begin() ; it != mapped.end(); it++)
            {
                outputFile<<it->first<<","<<it->second<<endl;
            }
        } else{
            //cout<<"nai na re bhava"<<endl;
        }
        outputFile.close();

    }
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
	//std::cout << "Dummy emit by BaseReducerInternal: " << key << ", " << val << std::endl;
    map<string,string>::iterator it = mapped.find(key);
    if(it == mapped.end())
    {
        mapped[key]=val;
    }
    else
    {
        //if(key=="Athens")
        //cout<<"Prev value = "<<mapped[key]<<endl;
        mapped[key] = to_string(stoi(mapped[key]) + stoi(val));
        //if(key=="Athens")
        //cout<<"New value = "<<mapped[key]<<endl;
    }
}
