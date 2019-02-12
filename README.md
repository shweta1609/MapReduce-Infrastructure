# cs6210Project4
MapReduce Infrastructure

1.In this project we have implemented a simplified mapreduce framework using GRPC for wordcount problem.
2.We have used Async GRPC communication between master thread and worker threads for performing mapping and reducing functions.
3.Parameters specified in config.ini are initialized in mapreduce_spec.h. They are also validated based on empty values and data types.
4.Required MapReduce Service and messages are defined in masterworker.proto file. We have created two messages each for Mapping and Reducing functions.
5.In master.h, we are initiating an RPC connection between master and worker threads. Once the connection is successful, master assigns MapperTasks to workers.
6.Each mapper function is producing n_output_files, which are then fed to the reducer, which aggregates the results from all these files and combines it into one.
7.We have also created a function_timing which calculates the time taken by thread to execute and accordingly updates its status to AVAILABLE, BUSY or UNAVAILABLE.
8.The program logic does not delete old files on execution. Therefore, best practice would be to delete all files in output folder especially if testcases involves change in output files.
