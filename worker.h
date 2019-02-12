#pragma once

#include <mr_task_factory.h>
#include "mr_tasks.h"

#include <vector>
#include <memory>
#include <thread>
#include <mutex>
#include <unistd.h>


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
using grpc::ServerBuilder;


using masterworker::MapReduce;
using masterworker::MapperQuery;
using masterworker::MapperReply;
using masterworker::GetFileShard;

using namespace std;
extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
        bool run();

	private:


    class CallData {
    public:
        // Take in the "service" instance (in this case representing an asynchronous
        // server) and the completion queue "cq" used for asynchronous communication
        // with the gRPC runtime.
        CallData(masterworker::MapReduce::AsyncService* service, ServerCompletionQueue* cq)
                : service_(service), cqs(cq), responder_(&ctx_), status_(CREATE) {
            // Invoke the serving logic right away.
            Proceed();
        }

        void Proceed() {
            if (status_ == CREATE) {
                // Make this instance progress to the PROCESS state.
                status_ = PROCESS;

                // As part of the initial CREATE state, we *request* that the system
                // start processing SayHello requests. In this request, "this" acts are
                // the tag uniquely identifying the request (so that different CallData
                // instances can serve different requests concurrently), in this case
                // the memory address of this CallData instance.
                //cout<<"registerdddddd"<<endl;
                service_->RequestAssignMapper(&ctx_, &request_, &responder_, cqs, cqs,
                                             this);

            } else if (status_ == PROCESS) {
                //cout<<"ajdkjsbckjzbczxcb\n";
                new CallData(service_, cqs);

//                cout<<"shards = "<<request_.shards_size()<<endl;
//                cout<<"start = "<<request_.shards(0).offset_start()<<endl;
//                cout<<"end = "<<request_.shards(0).offset_end()<<endl;
//
//                cout<<"start = "<<request_.shards(1).offset_start()<<endl;
//                cout<<"end = "<<request_.shards(1).offset_end()<<endl;
                if(request_.m_r()==0) {
                    cout<<"Mapping..."<<endl;
                    shared_ptr <BaseMapper> mapper = get_mapper_from_task_factory(request_.user_id());
                    mapper->impl_->init_params(request_.num_output());
                    for (int i = 0; i < request_.shards_size(); i++) {
                        int star = request_.shards(i).offset_start();
                        int en = request_.shards(i).offset_end();
                        string file_name = request_.shards(i).shard_path();
                        //cout << "filename = " << file_name << endl;
                        ifstream ifile;
                        ifile.open(file_name);
                        string output;
                        if (ifile.is_open()) {
                            //cout << "open jhali re" << endl;
                            ifile.seekg(star, ifile.beg);
                            while (getline(ifile, output)) {
                                if (ifile.tellg() <= en) {
                                    //cout << output << endl;
                                    mapper->map(output);
                                } else {
                                    break;
                                }

                            }
                            //cout << "band pan jhali re" << endl;
                            mapper->impl_->write_intoFile(request_.output_dir() + "/mapper-" + to_string(request_.shard_id()));

                        } else {
                            //cout << "nai n bhava" << endl;
                        }
                        ifile.close();
                    }

                    cout<<"Mapping done!"<<endl;
                }
                else
                {
                    int shard_id = request_.shard_id();
                    shared_ptr <BaseReducer> reducer = get_reducer_from_task_factory(request_.user_id());
                    cout<<"Reducing..."<<endl;
                    for(int i =0;i<request_.num_output();i++)
                    {

                        //cout<<"\n NUm output = "<< request_.num_output()<<endl<<endl;
                        string file_tosearch = request_.output_dir()+"/mapper-"+to_string(i)+"_"+to_string(request_.shard_id())+".txt";
                        //cout<<"file ========== "<<file_tosearch<<endl;
                        ifstream ifile;
                        ifile.open(file_tosearch);
                        string output;
                        string key;
                        vector<string> value;
                        if (ifile.is_open()) {
                            while (getline(ifile, output)) {
                                key = output.substr(0,output.find(","));
                                value.push_back(output.substr(output.find(",")+1,output.size()));
                                //cout<<"key = "<<key << " value = "<<value[0]<<endl;
                                reducer->reduce(key,value);
                                value.clear();
                            }
                        }
                        else
                        {
                            //cout<<"openach nai jhali"<<endl;
                        }
                    }
                    //cout<<"reducing done for file #"<<shard_id<<endl;
                    reducer->impl_->write_intoFile(request_.output_dir() +"/reducer-"+to_string(shard_id));
                    //mapper->impl_->write_intoFile();
                        //cout<<"shard for reduction = "<<request_.shard_id()<<endl;
                    cout<<"Reducing done!"<<endl;
                }
                reply_.set_success(true);
                responder_.Finish(reply_, Status::OK, this);
                status_ = FINISH;

            } else {
                GPR_ASSERT(status_ == FINISH);
                // Once in the FINISH state, deallocate ourselves (CallData).
                delete this;
            }
        }

    private:
        // The means of communication with the gRPC runtime for an asynchronous
        // server.
        masterworker::MapReduce::AsyncService* service_;
        // The producer-consumer queue where for asynchronous server notifications.
        ServerCompletionQueue* cqs;
        // Context for the rpc, allowing to tweak aspects of it such as the use
        // of compression, authentication, as well as to send metadata back to the
        // client.
        ServerContext ctx_;
        ClientContext client_ctx_;
        // What we get from the client.
        MapperQuery request_;
        // What we send back to the client.
        MapperReply reply_;

        // The means to get back to the client.
        ServerAsyncResponseWriter<MapperReply> responder_;


        // Let's implement a tiny state machine with the following states.
        enum CallStatus { CREATE, PROCESS, FINISH };
        CallStatus status_;  // The current serving state.
    };
    std::unique_ptr<ServerCompletionQueue> cq_;
    MapReduce::AsyncService service_;
    std::unique_ptr<Server> server_;
		/* NOW you can add below, data members and member functions as per the need of your implementation*/

};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) {
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(ip_addr_port, grpc::InsecureServerCredentials());
    // Register "service_" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *asynchronous* service.
    builder.RegisterService(&service_);
    // Get hold of the completion queue used for the asynchronous communication
    // with the gRPC runtime.
    cq_ = builder.AddCompletionQueue();
    // Finally assemble the server.
    server_ = builder.BuildAndStart();
    cout << "Server listening on " << ip_addr_port << std::endl;
}



/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	/*  Below 5 lines are just examples of how you will call map and reduce
		Remove them once you start writing your own logic */ 
	//std::cout << "worker.run(), I 'm not ready yet" <<std::endl;
	//auto mapper = get_mapper_from_task_factory("cs6210");
	//mapper->map("I m just a 'dummy', a \"dummy line\"");
	//auto reducer = get_reducer_from_task_factory("cs6210");
	//reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));

    new CallData(&service_, cq_.get());
    void* tag;
    bool ok;
    while (true) {

        GPR_ASSERT(cq_->Next(&tag, &ok));

        GPR_ASSERT(ok);

        static_cast<CallData*>(tag)->Proceed();

    }
	return true;
}
