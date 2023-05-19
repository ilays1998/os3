//
// Created by ilaysoffer on 5/18/23.
//
#ifndef MAPREDUCEFRAMEWORK_H
#define MAPREDUCEFRAMEWORK_H

#include "MapReduceClient.h"
#include "pthread.h"
#include "Resources/SampleClient/SampleClient.cpp"
#include "semaphore.h"
#include <atomic>


typedef void* JobHandle;


enum stage_t {UNDEFINED_STAGE=0, MAP_STAGE=1, SHUFFLE_STAGE=2, REDUCE_STAGE=3};

typedef struct {
    stage_t stage;
    float percentage;
} JobState;

struct JobContext {
    JobState* myState;
    pthread_t* threads;
    pthread_mutex_t* pthreadMutex;
    sem_t* sem;
    std::atomic<int>* atomic_counter;
};

struct ThreadContext {
    pthread_t thisThread;
    const MapReduceClient* mapReduceClient;
    int startIndex, endIndex;
    const InputVec* inputVec;
    IntermediateVec* threadIntermediateVec;
    std::atomic<int>* atomic_counter;
    //OutputVec* threadOutputVec;
};


void emit2 (K2* key, V2* value, void* context) {
    ThreadContext* threadContext = (ThreadContext*) context;
    IntermediatePair* curPair = new IntermediatePair (key, value);
    threadContext->threadIntermediateVec->push_back(*curPair);
    //TODO maybe add mutex/barrie/sam
    (*(threadContext->atomic_counter))++;
}

void emit3 (K3* key, V3* value, void* context);

void* mapWraper(void* arg){
    //pthread_mutex_lock(&mtx);
    ThreadContext* threadContext = (ThreadContext*) arg;
    for (int i = threadContext->startIndex ; i < threadContext->endIndex; i++) {
        threadContext->mapReduceClient->map(threadContext->inputVec->at(i).first,
                                            threadContext->inputVec->at(i).second,
                                            threadContext);
    }

    //pthread_mutex_unlock(&mtx);
}
JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel) {
    std::atomic<int> atomic_counter(0);
    pthread_t* threads = new pthread_t[multiThreadLevel];

    JobContext *jobContext = new JobContext;
    jobContext->threads = threads;
    jobContext->myState = new JobState[multiThreadLevel];
    jobContext->atomic_counter = &atomic_counter;
    int pairsForThread = inputVec.size()/multiThreadLevel;
    for (int i = 0; i < multiThreadLevel; i++) {
        jobContext->myState[i].stage = UNDEFINED_STAGE;
        ThreadContext* threadContext = new ThreadContext;
        threadContext->thisThread = threads[i];
        threadContext->mapReduceClient = &client;
        threadContext->inputVec = &inputVec;
        threadContext->threadIntermediateVec = new IntermediateVec;
        threadContext->startIndex = i * pairsForThread;
        threadContext->atomic_counter = &atomic_counter;
        if (i == multiThreadLevel-1){
            threadContext->endIndex = inputVec.size();
        }
        else{
            threadContext->endIndex = threadContext->startIndex + pairsForThread; // TODO check indexs
        }
        pthread_create(threads + i, NULL, mapWraper, threadContext);
    }
    return static_cast<JobHandle>(jobContext);
}

void waitForJob(JobHandle job);
void getJobState(JobHandle job, JobState* state);
void closeJobHandle(JobHandle job);


#endif //MAPREDUCEFRAMEWORK_H
