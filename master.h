#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"
#include <vector>
#include <memory>
#include <thread>
#include <mutex>
#include <unistd.h>
#include <ctime>
#include <chrono>

#include "masterworker.pb.h"
#include "masterworker.grpc.pb.h"
#include "threadpool.h"
#include <grpc++/grpc++.h>
#include <grpc++/server.h>
#include <grpc++/server_builder.h>
#include <grpc++/server_context.h>
#include <grpcpp/security/credentials.h>

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;
using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;

using masterworker::MapReduce;
using masterworker::MapperQuery;
using masterworker::MapperReply;
using masterworker::GetFileShard;

using namespace std;

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
enum status{
    Unavailable=0,
    Available=1,
    Busy=2
};

class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();



		struct Thread_status{
		    int thread_id;
		    status stat;
		};

	private:
    bool stop_timer;
    bool mapping;

    vector<Thread_status> thread_status;
	vector<unique_ptr< MapReduce::Stub>> stubs;

    vector<int> map_work;
    vector<bool> map_work_status;
    vector<bool> map_work_state;
    vector<int> map_curr_thread;

    vector<int> reduce_work;
    vector<bool> reduce_work_status;
    vector<bool> reduce_work_state;
    vector<int> reduce_curr_thread;

    int workleft;
    bool workleft_flag;
    mutex lock_workleft;


    vector<FileShard> fs;
    MapReduceSpec mrs;

    mutex lock_task;
    void function_map(int stub_index)
    {
        while(workleft) {
            bool gotit=false;
            int work_no;
            //cout<<"map work size = "<<map_work.size()<<endl;
            lock_task.lock();
            for(int i =0 ;i< map_work.size();i++)
            {
             if(map_work_state[i] == false)
             {
                 map_curr_thread[i]=stub_index;
                 map_work_state[i]=true;
                 work_no = map_work[i];
                 gotit=true;
                 break;
             }
            }
            lock_task.unlock();
            //cout<<"unlocl kela"<<endl;
            if(gotit)
            {
                thread_status[stub_index].stat=Busy;
                //cout<<"mapper no = "<<stub_index<<endl;
                CompletionQueue cq;
                Status status;
                ClientContext ctx;
                MapperQuery mq;
                MapperReply mr;
                mq.set_num_output(mrs.n_output_files);
                mq.set_output_dir("./Intermediate_files");

                //https://developers.google.com/protocol-buffers/docs/cpptutorial - Reference
                for(int i=0;i<fs[work_no].files.size();i++) {
                    GetFileShard *gfs = mq.add_shards();
                    gfs->set_shard_path(fs[work_no].files[i]);
                    gfs->set_offset_start(fs[work_no].offset_start[i]);
                    gfs->set_offset_end(fs[work_no].offset_end[i]);
                }
                mq.set_shard_id(work_no);
                mq.set_num_output(mrs.n_output_files);
                mq.set_output_dir(mrs.output_dir);
                mq.set_m_r(0);
                mq.set_user_id(mrs.user_id);
                std::unique_ptr<ClientAsyncResponseReader<MapperReply> > async_rpc(
                        stubs[stub_index]->PrepareAsyncAssignMapper(&ctx, mq, &cq));
                async_rpc->StartCall();
                async_rpc->Finish(&mr, &status, (void *) (size_t) stub_index);
                chrono::high_resolution_clock::time_point start_time = (chrono::high_resolution_clock::now());
                void *got_tag;
                bool ok = false;
                cq.Next(&got_tag, &ok);
                if(ok)
                {
                    if(mr.success()) {
                        map_work_status[work_no] = true;
                        //cout<<"1 work done"<<endl;
                        lock_workleft.lock();
                        workleft--;
                        if(workleft==0)
                            workleft_flag=false;
                        lock_workleft.unlock();
                    }
                    else
                    {
                        map_work_state[work_no] = false;
                    }
                }
                chrono::high_resolution_clock::time_point end_time = (chrono::high_resolution_clock::now());
                chrono::duration<double> time_span = chrono::duration_cast<chrono::duration<double>>(end_time-start_time);
                cout<<"Worker #"<<stub_index<<" took "<<time_span.count()<<" microseconds for mapping"<<endl;
                thread_status[stub_index].stat=Available;
            }
            else
            {

            }

        }
    }



    void function_reduce(int stub_index)
    {
        while(workleft) {
            bool gotit=false;
            int work_no;
            //cout<<"map work size = "<<map_work.size()<<endl;
            lock_task.lock();
            for(int i =0 ;i< mrs.n_output_files;i++)
            {
                if(reduce_work_state[i] == false)
                {
                    reduce_curr_thread[i]=stub_index;
                    reduce_work_state[i]=true;
                    work_no = reduce_work[i];
                    gotit=true;
                    break;
                }
            }
            lock_task.unlock();
            //cout<<"unlocl kela"<<endl;
            if(gotit)
            {
                thread_status[stub_index].stat=Busy;
                //cout<<"mapper no = "<<stub_index<<endl;
                CompletionQueue cq;
                Status status;
                ClientContext ctx;
                MapperQuery mq;
                MapperReply mr;
                mq.set_shard_id(work_no);
                mq.set_m_r(1);
                mq.set_output_dir(mrs.output_dir);
                mq.set_num_output(fs.size());
                mq.set_user_id(mrs.user_id);
                std::unique_ptr<ClientAsyncResponseReader<MapperReply> > async_rpc(
                        stubs[stub_index]->PrepareAsyncAssignMapper(&ctx, mq, &cq));
                async_rpc->StartCall();
                async_rpc->Finish(&mr, &status, (void *) (size_t) stub_index);

                chrono::high_resolution_clock::time_point start_time = (chrono::high_resolution_clock::now());
                void *got_tag;
                bool ok = false;
                cq.Next(&got_tag, &ok);
                if(ok)
                {
                    if(mr.success()) {
                        reduce_work_status[work_no] = true;
                        //cout<<"1 work done"<<endl;
                        lock_workleft.lock();
                        workleft--;
                        if(workleft==0)
                            workleft_flag=false;
                        lock_workleft.unlock();
                    }
                    else
                    {
                        reduce_work_state[work_no] = false;
                    }
                }
                chrono::high_resolution_clock::time_point end_time = (chrono::high_resolution_clock::now());
                chrono::duration<double> time_span = chrono::duration_cast<chrono::duration<double>>(end_time-start_time);
                cout<<"Worker #"<<stub_index<<" took "<<time_span.count()<<" microseconds for reducing"<<endl;
                thread_status[stub_index].stat = Available;
            }
            else
            {

            }

        }
    }


    void function_timing()
    {
        while(!stop_timer)
        {
            if(mapping)
            {
                vector<long int> prevTime;
                vector<bool> prevtime_set;
                //vector<long int> newTime;
                //vector<bool> newTime_set;
                prevTime.reserve(map_work.size());
                //newTime.reserve(map_work.size());
                prevtime_set.reserve(map_work.size());
                //newTime_set.reserve(map_work.size());

                for(int i =0 ;i< map_work.size();i++)
                {
                    if(map_work_state[i]==true && map_work_status[i]==false) {
                        //time_t abc =
                        if(prevtime_set[i]==false) {
                            prevTime[i] = static_cast<long int> (time(nullptr));
                            prevtime_set[i] = true;
                        }
                        else
                        {
                            if(map_work_status[i]==false && map_work_state[i]==true && static_cast<long int> (time(nullptr)) - prevTime[i] > 1000000)
                            {
                                thread_status[map_curr_thread[i]].stat = Unavailable;
                                prevtime_set[i]=false;
                            }
                        }
                        //cout<<"Time bagha time = "<<prevTime[i]<<endl;

                    }
                }
                // map_work

            }
            else
            {
                vector<long int> prevTime;
                vector<bool> prevtime_set;
                //vector<long int> newTime;
                //vector<bool> newTime_set;
                prevTime.reserve(reduce_work.size());
                //newTime.reserve(map_work.size());
                prevtime_set.reserve(reduce_work.size());
                //newTime_set.reserve(map_work.size());

                for(int i =0 ;i< reduce_work.size();i++)
                {
                    if(reduce_work_state[i]==true && reduce_work_status[i]==false) {
                        //time_t abc =
                        if(prevtime_set[i]==false) {
                            prevTime[i] = static_cast<long int> (time(nullptr));
                            prevtime_set[i] = true;
                        }
                        else
                        {
                            if(reduce_work_status[i]==false && reduce_work_state[i]==true && static_cast<long int> (time(nullptr)) - prevTime[i] > 1000000)
                            {
                                //cout<<"\nUnavailable"<<endl<<endl;
                                thread_status[reduce_curr_thread[i]].stat = Unavailable;
                                prevtime_set[i]=false;
                            }
                        }
                        //cout<<"Time bagha time = "<<prevTime[i]<<endl;

                    }
                }
            }
        }
    }
    /* NOW you can add below, data members and member functions as per the need of your implementation*/

};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {
    for(int i=0;i<mr_spec.worker_ipaddr_ports.size();i++) {
        //cout<<"mapper = "<<mr_spec.worker_ipaddr_ports[i]<<endl;
        shared_ptr <grpc::ChannelInterface> channel = grpc::CreateChannel(mr_spec.worker_ipaddr_ports[i],
                                                                          grpc::InsecureChannelCredentials());
        stubs.push_back(MapReduce::NewStub(channel));
    }
    //cout<<"shard size = "<<file_shards.size()<<endl;
    workleft=file_shards.size();
    workleft_flag = true;
    for(int i=0;i<file_shards.size();i++)
    {
        map_work.push_back(i);
        map_work_status.push_back(false);
        map_work_state.push_back(false);
        map_curr_thread.push_back(-1);
    }

    for(int i=0;i<mr_spec.n_output_files;i++)
    {
        reduce_work.push_back(i);
        reduce_work_status.push_back(false);
        reduce_work_state.push_back(false);
        reduce_curr_thread.push_back(-1);
    }

    fs = file_shards;
    mrs = mr_spec;
}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {

    thread pool[mrs.n_workers];
    thread_status.reserve(mrs.n_workers);
    stop_timer=false;
    thread timing_info = thread(&Master::function_timing,this);

    for(int i=0;i<mrs.n_workers;i++)
    {
        thread_status[i].thread_id =i;
        thread_status[i].stat = Available;
    }
    mapping =true;
    cout<<"Mapping Started..."<<endl;
    for(int i =0;i<mrs.n_workers;i++)
    {
        pool[i] = thread(&Master::function_map,this,i);
    }
    for(int i =0;i<mrs.n_workers;i++)
    {
        pool[i].join();
    }
    cout<<"Mapping done!"<<endl;
    workleft=mrs.n_output_files;
    workleft_flag = true;
    cout<<"Reducing Started..."<<endl;
    mapping = false;
    for(int i =0;i<mrs.n_workers;i++)
    {
        pool[i] = thread(&Master::function_reduce,this,i);
    }
    for(int i =0;i<mrs.n_workers;i++)
    {
        pool[i].join();
    }
    cout<<"Reducing Done!"<<endl;
    stop_timer=true;
    timing_info.join();

	return true;
}