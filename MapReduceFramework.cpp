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
#include <algorithm>
#include <mutex>


typedef void* JobHandle;

//TODO implement stages
enum stage_t {UNDEFINED_STAGE=0, MAP_STAGE=1, SHUFFLE_STAGE=2, REDUCE_STAGE=3};

typedef struct {
    stage_t stage;
    float percentage;
} JobState;


class JobContext;

struct ThreadContext {
    int ID;
    pthread_t thisThread;
    const MapReduceClient* mapReduceClient;
    int startIndex, endIndex;
    const InputVec* inputVec;
    IntermediateVec* threadIntermediateVec;
    std::atomic<int>* atomic_counter;
    JobContext *globalJobContext;
    //OutputVec* threadOutputVec;
};

struct JobContext {
    JobState* myState;
    ThreadContext *threadsContext;
    pthread_mutex_t* pthreadMutex;
    sem_t sem;
    std::atomic<int>* atomic_counter;
    IntermediateVec* intermediateVec;
    int numOfThreads;
    //std::mutex insertIntermediateVecsMutex;
};

bool compareIntermediateVec(const IntermediatePair& a, const IntermediatePair& b){
    return *(a.first) < *(b.first);
}

void sortIntermediateVec(ThreadContext* threadContext){
    std::sort(threadContext->threadIntermediateVec->begin(),
              threadContext->threadIntermediateVec->end(),
              compareIntermediateVec);
}


/*
void insertIntermediateVecs(JobContext* jobContext,
                            ThreadContext* threadContext){
    std::lock_guard<std::mutex> lock(jobContext->insertIntermediateVecsMutex);
    auto lastIndex = jobContext->intermediateVec->begin();
    for (int i = 0; i < threadContext->threadIntermediateVec->size(); i++){
        auto it = std::lower_bound(lastIndex,
                                       jobContext->intermediateVec->end(),
                                       threadContext->threadIntermediateVec->at(i),
                                       compareIntermediateVec);
        jobContext->intermediateVec->insert(it, threadContext->threadIntermediateVec->at(i));
        lastIndex = it; //TODO make sure its logical
    }
    //std::lock_guard<std::mutex> unlock(jobContext->insertIntermediateVecsMutex); //TODO check if neccasry
}
*/
K2* findLargestKey(JobContext* jobContext) {
    ThreadContext* tc = (ThreadContext*) (jobContext->threadsContext);
    K2* large = nullptr;
    for (int i = 0; i < jobContext->numOfThreads; i++){
        ThreadContext* tc = (ThreadContext*) (jobContext->threadsContext + i); //TODO check
        if (tc->threadIntermediateVec->empty())
            continue;
        IntermediatePair *temp = &tc->threadIntermediateVec->back();
        if (large == nullptr || large < temp->first){
            large = temp->first;
        }
    }
    return large;
}

void insertIntermediateVecs(JobContext* jobContext, ThreadContext* threadContext){
    if (threadContext->ID != 0){
        sem_wait(&(jobContext->sem));
    }
    else{
        while (true){
            K2 *curLargestKey = findLargestKey(jobContext);
            if (curLargestKey == nullptr){
                break;
            }
            for (int i = 0; i < jobContext->numOfThreads; i++){
                ThreadContext* tc = (ThreadContext*) (jobContext->threadsContext + i); //TODO check
                IntermediatePair temp = tc->threadIntermediateVec->back();
                if (!(temp.first < curLargestKey) &&
                        !(curLargestKey < temp.first)){
                    jobContext->intermediateVec->insert(jobContext->intermediateVec->begin(), temp);
                    tc->threadIntermediateVec->pop_back();
                }
            }
        }
        sem_post(&(jobContext->sem));
    }
}

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
    sortIntermediateVec(threadContext);
    insertIntermediateVecs(threadContext->globalJobContext, threadContext);
    //pthread_mutex_unlock(&mtx);
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel) {
    std::atomic<int> atomic_counter(0);
    ThreadContext allThreadsContext[multiThreadLevel];
    pthread_t threads[multiThreadLevel];
    JobContext *jobContext = new JobContext;
    jobContext->threadsContext = allThreadsContext;
    jobContext->myState = new JobState[multiThreadLevel];
    jobContext->atomic_counter = &atomic_counter;
    jobContext->intermediateVec = new IntermediateVec;
    jobContext->numOfThreads = multiThreadLevel;
    int pairsForThread = inputVec.size()/multiThreadLevel;
    for (int i = 0; i < multiThreadLevel; i++) {
        jobContext->myState[i].stage = UNDEFINED_STAGE;
        //ThreadContext* threadContext = new ThreadContext;
        allThreadsContext[i].globalJobContext = jobContext;
        allThreadsContext[i].ID = i;
        allThreadsContext[i].thisThread = threads[i];
        allThreadsContext[i].mapReduceClient = &client;
        allThreadsContext[i].inputVec = &inputVec;
        allThreadsContext[i].threadIntermediateVec = new IntermediateVec;
        allThreadsContext[i].startIndex = i * pairsForThread;
        allThreadsContext[i].atomic_counter = &atomic_counter;
        if (i == multiThreadLevel-1){
            allThreadsContext[i].endIndex = inputVec.size();
        }
        else{
            allThreadsContext[i].endIndex = allThreadsContext[i].startIndex + pairsForThread; // TODO check indexs
        }
        pthread_create(threads + i, NULL, mapWraper, &allThreadsContext[i]);
    }
    for (int i = 0; i < multiThreadLevel; i++){
        pthread_join(allThreadsContext[i].thisThread, NULL);
    }

    //
    return static_cast<JobHandle>(jobContext);
}

void waitForJob(JobHandle job);
void getJobState(JobHandle job, JobState* state);
void closeJobHandle(JobHandle job);


#endif //MAPREDUCEFRAMEWORK_H
