//
// Created by ilaysoffer on 5/18/23.
//
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include "pthread.h"
#include "Resources/SampleClient/SampleClient.cpp"
#include "semaphore.h"
#include <atomic>
#include <algorithm>
#include "Resources/Barrier/Barrier.h"

typedef void* JobHandle;

//TODO implement stages

typedef std::vector<IntermediateVec> VecOfIntermediateVec;


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
    pthread_mutex_t pthreadMutex;
    sem_t sem;
    std::atomic<int>* atomic_counter;
    //IntermediateVec* intermediateVec;
    VecOfIntermediateVec *vecOfIntermediateVec;
    int numOfThreads;
    Barrier* barrier;


    //std::mutex insertIntermediateVecsMutex;
    OutputVec *outputVec;
};

bool compareIntermediatePair(const IntermediatePair& a, const IntermediatePair& b){
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


/*
void insertIntermediateVecs(JobContext* jobContext,
                            ThreadContext* threadContext){
    std::lock_guard<std::mutex> lock(jobContext->insertIntermediateVecsMutex);
    auto lastIndex = jobContext->intermediateVec->begin();
    for (int i = 0; i < threadContext->threadIntermediateVec->size(); i++){
        auto it = std::lower_bound(lastIndex,
                                       jobContext->intermediateVec->end(),
                                       threadContext->threadIntermediateVec->at(i),
                                       compareIntermediatePair);
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
            IntermediateVec intermediateVecTemp;
            for (int i = 0; i < jobContext->numOfThreads; i++){
                ThreadContext* tc = (ThreadContext*) (jobContext->threadsContext + i); //TODO check
                IntermediatePair temp = tc->threadIntermediateVec->back();
                if (!(temp.first < curLargestKey) &&
                        !(curLargestKey < temp.first)){

                    intermediateVecTemp.push_back(temp);
                    tc->threadIntermediateVec->pop_back();
                }
            }
            jobContext->vecOfIntermediateVec->insert(jobContext->vecOfIntermediateVec->begin(),
                                                     intermediateVecTemp);
            (*(jobContext->atomic_counter))++;
            //intermediateVecTemp.clear(); //TODO maybe
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

void emit3 (K3* key, V3* value, void* context) {
    ThreadContext* pContext = (ThreadContext*) context;

    pthread_mutex_lock(&pContext->globalJobContext->pthreadMutex);
    OutputPair newPair(key, value);
    auto it = std::lower_bound(pContext->globalJobContext->outputVec->begin(), pContext->globalJobContext->outputVec->begin()
                               , newPair, compareOutputPair);
    pContext->globalJobContext->outputVec->insert(it, newPair);
    pContext->mapReduceClient->reduce(pContext->threadIntermediateVec, pContext);
}

void reducePhase(ThreadContext *pContext);

void* mapWraper(void* arg){
    //pthread_mutex_lock(&mtx);
    ThreadContext* threadContext = (ThreadContext*) arg;
    //TODO: check if this is the best way to split the job
    for (int i = threadContext->startIndex ; i < threadContext->endIndex; i++) {
        threadContext->mapReduceClient->map(threadContext->inputVec->at(i).first,
                                            threadContext->inputVec->at(i).second,
                                            threadContext);
    }

    sortIntermediateVec(threadContext);
    threadContext->globalJobContext->barrier->barrier();
    //end barrier

    insertIntermediateVecs(threadContext->globalJobContext, threadContext);
    threadContext->globalJobContext->barrier->barrier(); //TODO check if needed

    //TODO: maybe need in loop
    reducePhase(threadContext);
    //TODO: maybe need barrier
    //pthread_mutex_unlock(&mtx);
}

void reducePhase(ThreadContext *pContext) {
    pthread_mutex_lock(&pContext->globalJobContext->pthreadMutex);
    pContext->threadIntermediateVec = &pContext->globalJobContext->vecOfIntermediateVec->back();
    pContext->globalJobContext->vecOfIntermediateVec->pop_back();
    pthread_mutex_unlock(&pContext->globalJobContext->pthreadMutex);

    pContext->mapReduceClient->reduce(pContext->threadIntermediateVec, pContext);


}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel) {
    Barrier barrier(multiThreadLevel);
    std::atomic<int> atomic_counter(0);
    ThreadContext allThreadsContext[multiThreadLevel];
    pthread_t threads[multiThreadLevel];
    JobContext *jobContext = new JobContext;
    if (pthread_mutex_init(&jobContext->pthreadMutex, NULL) != 0)
    {
        return NULL; //TODO: ERROR
    }
    jobContext->barrier = &barrier;
    jobContext->outputVec = &outputVec;
    jobContext->threadsContext = allThreadsContext;
    jobContext->myState = new JobState[multiThreadLevel];
    jobContext->atomic_counter = &atomic_counter;
    jobContext->vecOfIntermediateVec = new VecOfIntermediateVec ;
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
