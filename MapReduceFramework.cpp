//
// Created by ilaysoffer on 5/18/23.
//
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include "pthread.h"
#include <atomic>
#include <algorithm>
#include "Barrier.h"
#include <cmath>
#include <cstdio>
#include <iostream>

#define ERROR_MSG "system error: "

typedef std::vector<IntermediateVec> VecOfIntermediateVec;

struct ThreadContext;

struct JobContext {
    int numOfIntermediatePairs = 0;
    JobState myState;
    std::vector<ThreadContext> vecOfThreads;
    std::atomic<bool>* flagWait;
    pthread_mutex_t pthreadMutex;
    std::atomic<int>* atomic_counter;
    std::atomic<int>* mapCompleted;
    std::atomic<int>* shuffleCompleted;
    std::atomic<int>* reduceCompleted;
    VecOfIntermediateVec *vecOfIntermediateVec;
    int numOfThreads;
    Barrier* barrier;
    OutputVec *outputVec;
    pthread_mutex_t pthreadMutexForEmit3;
    ThreadContext** arrayVec;
    pthread_t *p_treads;
    const InputVec *inputVec;
};

struct ThreadContext {
    int ID;
    pthread_t* thisThread;
    const MapReduceClient* mapReduceClient;
    IntermediateVec* threadIntermediateVec;
    IntermediateVec* threadReduceIntermediateVec;
    JobContext *globalJobContext;
    long unsigned int oldAtomicVal;
};
bool compareIntermediatePair(const IntermediatePair& a, const IntermediatePair& b){
//    if (a.first == nullptr || b.first == nullptr) {
//        // Handle the case where either a.first or b.first is null
//        // Return a comparison result based on your requirements
//        // For example, you could return true if a.first is null and false otherwise
//        return (a.first == nullptr);
//    }
    return *(a.first) < *(b.first);
}

bool compareOutputPair(const OutputPair & a, const OutputPair& b){
    return *(a.first) < *(b.first);
}

void sortIntermediateVec(ThreadContext* threadContext){
    std::sort(threadContext->threadIntermediateVec->begin(),
              threadContext->threadIntermediateVec->end(),
              compareIntermediatePair);
}

K2* findLargestKey(JobContext* jobContext) {
    ThreadContext *tc;
    K2* large = nullptr;
    for (int i = 0; i < jobContext->numOfThreads; i++){
        tc = (ThreadContext*) (&jobContext->vecOfThreads.at(i));
        if (tc->threadIntermediateVec->empty())
            continue;
        IntermediatePair temp = tc->threadIntermediateVec->back();
        if (large == nullptr || *large < *temp.first){
            large = temp.first;
        }
    }
    return large;
}

void insertIntermediateVecs(JobContext* jobContext, ThreadContext* threadContext){
    if (threadContext->ID != 0){

    }
    else{
        threadContext->globalJobContext->myState.stage = SHUFFLE_STAGE;
        //threadContext->globalJobContext->myState.percentage = 0;
        threadContext->globalJobContext->numOfIntermediatePairs = 0;
        for (auto it : jobContext->vecOfThreads){
            threadContext->globalJobContext->numOfIntermediatePairs +=
                    it.threadIntermediateVec->size();
        }
        while (true){
            K2 *curLargestKey = findLargestKey(jobContext);
            if (curLargestKey == nullptr){
                threadContext->globalJobContext->myState.stage = REDUCE_STAGE;
                //threadContext->globalJobContext->myState.percentage = 0.0;
                break;
            }
            IntermediateVec intermediateVecTemp;
            for (ThreadContext tc : jobContext->vecOfThreads){
                while (true){
                    if (tc.threadIntermediateVec->empty() ||
                    (*tc.threadIntermediateVec->back().first < *curLargestKey) ||
                    (*curLargestKey < *tc.threadIntermediateVec->back().first))
                        break;
                    intermediateVecTemp.push_back(tc.threadIntermediateVec->back());
                    tc.threadIntermediateVec->pop_back();
                }
            }
            if (intermediateVecTemp.empty()) {
                continue;
            }
            jobContext->vecOfIntermediateVec->insert(jobContext->vecOfIntermediateVec->begin(),
                                                     intermediateVecTemp);
            (*threadContext->globalJobContext->shuffleCompleted) += intermediateVecTemp.size();
        }
    }
}

void emit2 (K2* key, V2* value, void* context) {
    ThreadContext* threadContext = (ThreadContext*) context;
    threadContext->threadIntermediateVec->push_back({key, value});
}

void emit3 (K3* key, V3* value, void* context) {
    ThreadContext* pContext = (ThreadContext*) context;

    OutputPair newPair(key, value);
    if(pthread_mutex_lock(&pContext->globalJobContext->pthreadMutexForEmit3) != 0){
        std::cout << ERROR_MSG << "couldn't lock mutex" << std::endl;
        exit(1);
    }
/*
    auto it = std::lower_bound(pContext->globalJobContext->outputVec->begin(),
                               pContext->globalJobContext->outputVec->end()
                               , newPair, compareOutputPair);
    pContext->globalJobContext->outputVec->insert(it, newPair); // TODO: check deep copy
*/
    pContext->globalJobContext->outputVec->push_back(newPair);
    if(pthread_mutex_unlock(&pContext->globalJobContext->pthreadMutexForEmit3) != 0){
        std::cout << ERROR_MSG << "couldn't unlock mutex" << std::endl;
        exit(1);
    }
}

void reducePhase(ThreadContext *pContext);

ThreadContext *initializeThreadContext(const MapReduceClient &client,
                                       JobContext *jobContext,
                                       const pthread_t *threads, int i);

void* mapWraper(void* arg){
    ThreadContext* threadContext = (ThreadContext*) arg;
    threadContext->globalJobContext->myState.stage = MAP_STAGE;
    while((threadContext->oldAtomicVal = (*threadContext->globalJobContext->atomic_counter)++)
    < threadContext->globalJobContext->inputVec->size()) {
        threadContext->mapReduceClient->map(
            threadContext->globalJobContext->inputVec->at(threadContext->oldAtomicVal).first,
            threadContext->globalJobContext->inputVec->at(threadContext->oldAtomicVal).second,
            threadContext);
        (*threadContext->globalJobContext->mapCompleted)++;
    }
    sortIntermediateVec(threadContext);
    threadContext->globalJobContext->barrier->barrier(); // Shouldnt be here but wont work otherwise
    insertIntermediateVecs(threadContext->globalJobContext, threadContext);
    threadContext->globalJobContext->barrier->barrier();
    reducePhase(threadContext);
    return 0;
}

void reducePhase(ThreadContext *pContext) {
    while (true) {
        if(pthread_mutex_lock(&pContext->globalJobContext->pthreadMutex)){
            std::cout << ERROR_MSG << "couldn't lock mutex" << std::endl;
            exit(1);
        }
        if (pContext->globalJobContext->vecOfIntermediateVec->empty()) {
            if (pthread_mutex_unlock(&pContext->globalJobContext->pthreadMutex) != 0) {

                std::cout << ERROR_MSG << "couldn't unlock mutex" << std::endl;
                exit(1);

            }
            return;
        }
        pContext->threadReduceIntermediateVec->assign(
                pContext->globalJobContext->vecOfIntermediateVec->back().begin(),
                pContext->globalJobContext->vecOfIntermediateVec->back().end());
        int sizeOfNewVec = pContext->threadReduceIntermediateVec->size();
        pContext->globalJobContext->vecOfIntermediateVec->pop_back();
        (*pContext->globalJobContext->reduceCompleted)+=sizeOfNewVec;
        if (pthread_mutex_unlock(&pContext->globalJobContext->pthreadMutex) != 0) {

            std::cout << ERROR_MSG << "couldn't lock mutex" << std::endl;
            exit(1);

        }
        pContext->mapReduceClient->reduce(pContext->threadReduceIntermediateVec,
                                          pContext);
    }
}
void initializeJobContext(JobContext* jobContext,
                        int multiThreadLevel,
                          const InputVec &inputVec,
                          OutputVec& outputVec){
    Barrier *barrier = new Barrier(multiThreadLevel);
    std::atomic<int> *atomic_counter = new std::atomic<int>(0);
    std::atomic<int> *mapCounter = new std::atomic<int>(0);
    std::atomic<int> *shuffleCounter = new std::atomic<int>(0);
    std::atomic<int> *reduceCounter = new std::atomic<int>(0);
    std::atomic<bool> *flagWait = new std::atomic<bool>(false);
    if (pthread_mutex_init(&jobContext->pthreadMutex, nullptr) != 0)
    {
        std::cout << ERROR_MSG << "couldn't create mutex" << std::endl;
        exit(1);
    }
    if (pthread_mutex_init(&jobContext->pthreadMutexForEmit3, nullptr) != 0)
    {
        std::cout << ERROR_MSG << "couldn't create mutex" << std::endl;
        exit(1);
    }
    jobContext->inputVec = &inputVec;
    jobContext->barrier = barrier;
    jobContext->outputVec = &outputVec;
    jobContext->atomic_counter = atomic_counter;
    jobContext->flagWait = flagWait;
    jobContext->mapCompleted = mapCounter;
    jobContext->shuffleCompleted = shuffleCounter;
    jobContext->reduceCompleted = reduceCounter;
    jobContext->vecOfIntermediateVec = new VecOfIntermediateVec;
    jobContext->numOfThreads = multiThreadLevel;
    jobContext->myState.stage = UNDEFINED_STAGE;
    //jobContext->myState.percentage = 0;
    jobContext->arrayVec = new ThreadContext*[multiThreadLevel];
}
ThreadContext *initializeThreadContext(const MapReduceClient &client,
                                       const InputVec &inputVec,
                                       JobContext *jobContext,
                                       int i) {
    auto *threadContext = new ThreadContext;
    threadContext->globalJobContext = jobContext;
    threadContext->ID = i;
    threadContext->mapReduceClient = &client;
    threadContext->threadIntermediateVec = new IntermediateVec;
    threadContext->threadReduceIntermediateVec = new IntermediateVec;
    return threadContext;
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel) {
    JobContext *jobContext = new JobContext;
    initializeJobContext(jobContext, multiThreadLevel, inputVec, outputVec);
    pthread_t *threads = new pthread_t[multiThreadLevel];
    jobContext->p_treads = threads;
    for (int i = 0; i < multiThreadLevel; ++i) {
        ThreadContext *threadContext = initializeThreadContext(client,
                                                               inputVec,
                                                               jobContext,
                                                               i);
        threadContext->thisThread = &threads[i];
        jobContext->vecOfThreads.push_back(*threadContext);
        jobContext->arrayVec[i] = threadContext;
    }
    for (int i = 0; i < multiThreadLevel; ++i) {
        if (pthread_create(&threads[i],
                       nullptr,
                       mapWraper,
                       &jobContext->vecOfThreads.at(i)) != 0){
            std::cout << ERROR_MSG << "couldn't create thread" << std::endl;
            exit(1);
        }
    }
    return static_cast<JobHandle>(jobContext);
}

void waitForJob(JobHandle job){
    JobContext* jobContext = (JobContext*) job;
    if (*jobContext->flagWait){
        return;
    }
    *jobContext->flagWait = true;
    for (long unsigned int i =0 ; i< jobContext->vecOfThreads.size(); i++){
        if(pthread_join(*jobContext->vecOfThreads.at(i).thisThread,
                        nullptr) != 0){
            std::cout << ERROR_MSG << "couldn't join threads" << std::endl;
            exit(1);
        }
    }
}

void getJobState(JobHandle job, JobState* state){
    JobContext* jobContext = (JobContext*) job;

    switch (jobContext->myState.stage) {
        case MAP_STAGE:
            state->percentage = ((float )(*jobContext->mapCompleted) /
                    (float)jobContext->inputVec->size()) * 100;
            state->stage = MAP_STAGE;
            return;
        case SHUFFLE_STAGE:
            state->percentage = (((float )(*jobContext->shuffleCompleted) /
                    (float)jobContext->numOfIntermediatePairs)) * 100;
            state->stage = SHUFFLE_STAGE;
            return;
        case REDUCE_STAGE:
            state->percentage = ((float )(*jobContext->reduceCompleted) /
                    (float)jobContext->numOfIntermediatePairs) * 100;
            state->stage = REDUCE_STAGE;
            return;
        case UNDEFINED_STAGE:
            state->percentage = 0;
            state->stage = UNDEFINED_STAGE;
            return;
    }

}

void closeJobHandle(JobHandle job){
    JobContext* jobContext = (JobContext*) job;
    waitForJob(job);
    for (int i = 0; i < jobContext->numOfThreads; i++) {
        delete jobContext->arrayVec[i]->threadIntermediateVec;
        delete jobContext->arrayVec[i]->threadReduceIntermediateVec;
        delete jobContext->arrayVec[i];
    }
    delete[] jobContext->arrayVec;
    delete[] jobContext->p_treads;
    //jobContext->vecOfThreads.clear();
    delete jobContext->atomic_counter;
    delete jobContext->mapCompleted;
    delete jobContext->reduceCompleted;
    delete jobContext->shuffleCompleted;
    delete jobContext->flagWait;
    delete jobContext->vecOfIntermediateVec;
    if(pthread_mutex_destroy(&jobContext->pthreadMutex) != 0){
        std::cout << ERROR_MSG << "couldn't destroy mutex" << std::endl;
        exit(1);
    }
    if(pthread_mutex_destroy(&jobContext->pthreadMutexForEmit3) != 0){
        std::cout << ERROR_MSG << "couldn't destroy mutex" << std::endl;
        exit(1);
    }
    delete jobContext->barrier;
    delete jobContext;
}
