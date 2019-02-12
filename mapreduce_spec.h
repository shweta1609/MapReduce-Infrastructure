#pragma once

#include <string>
#include <fstream>
#include <vector>
#include <sstream>


using namespace std;

/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
int n_workers;
vector<string> worker_ipaddr_ports;
vector<string> input_files;
string output_dir;
int n_output_files;
int map_kilobytes;
string user_id;
};


/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {
    ifstream config_file;
    config_file.open("config.ini");
    string output;
    if (config_file.is_open()) {
        while (getline(config_file, output)) {
            if(output.find("n_workers=")<output.size())
            {
                mr_spec.n_workers = stoi(output.substr(10,output.size()-1));
            }
            else if(output.find("output_dir=") < output.size())
            {
                mr_spec.output_dir = output.substr(11,output.size()-1);
                //cout<< mr_spec.output_dir;
            }
            else if(output.find("user_id=") < output.size())
            {
                mr_spec.user_id = output.substr(8,output.size()-1);
                //cout<< mr_spec.user_id;
            }
            else if(output.find("n_output_files=") < output.size())
            {
                mr_spec.n_output_files = stoi(output.substr(15,output.size()-1));
                //cout<< mr_spec.n_output_files;
            }
            else if(output.find("worker_ipaddr_ports=") < output.size())
            {
                stringstream id_list(output.substr(20,output.size()-1));
                string intermediate;
                while(getline(id_list, intermediate, ','))
                {
                    mr_spec.worker_ipaddr_ports.push_back(intermediate);
                }
            }
            else if(output.find("map_kilobytes=") < output.size())
            {
                mr_spec.map_kilobytes = stoi(output.substr(14,output.size()-1));
                //cout<< mr_spec.map_kilobytes;
            }
            else if(output.find("input_files=") < output.size())
            {
                stringstream id_list(output.substr(12,output.size()-1));
                string intermediate;
                while(getline(id_list, intermediate, ','))
                {
                    mr_spec.input_files.push_back(intermediate);
                }
            }

        }
    }
    else
    {
        return false;
    }


	return true;
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
    if(mr_spec.input_files.size() == 0)
        return false;
    if(mr_spec.map_kilobytes==0)
        return false;
    if(mr_spec.worker_ipaddr_ports.size()==0)
        return false;
    if(mr_spec.n_output_files==0)
        return false;
    if(mr_spec.user_id=="")
        return false;
    if(mr_spec.n_workers==0)
        return false;
    if(mr_spec.output_dir=="")
        return false;

	return true;
}
