//
// Created by ilaysoffer on 5/18/23.
//
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include "pthread.h"
#include "semaphore.h"
#include <atomic>
#include <algorithm>
#include "Resources/Barrier/Barrier.h"
#include <cmath>

//TODO implement stages

typedef std::vector<IntermediateVec> VecOfIntermediateVec;

struct ThreadContext;

struct JobContext {
    int numOfIntermediatePairs = 0;
    JobState myState;
    std::vector<ThreadContext> vecOfThreads;
    std::atomic<bool>* flagWait;
    //ThreadContext *threadsContext;
    pthread_mutex_t pthreadMutex;
    sem_t sem;
    std::atomic<int>* atomic_counter;
    std::atomic<int>* mapCompleted;
    std::atomic<int>* shuffleCompleted;
    std::atomic<int>* reduceCompleted;
    //IntermediateVec* intermediateVec;
    VecOfIntermediateVec *vecOfIntermediateVec;
    int numOfThreads;
    Barrier* barrier;
    //std::mutex insertIntermediateVecsMutex;
    OutputVec *outputVec;
};

struct ThreadContext {
    int ID;
    pthread_t* thisThread;
    const MapReduceClient* mapReduceClient;
    const InputVec* inputVec;
    IntermediateVec* threadIntermediateVec;
    IntermediateVec* threadReduceIntermediateVec;
    JobContext *globalJobContext;
    int oldAtomicVal;
    //OutputVec* threadOutputVec;
};
bool compareIntermediatePair(const IntermediatePair& a, const IntermediatePair& b){
    if (a.first == nullptr || b.first == nullptr) {
        // Handle the case where either a.first or b.first is null
        // Return a comparison result based on your requirements
        // For example, you could return true if a.first is null and false otherwise
        return (a.first == nullptr);
    }
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
        tc = (ThreadContext*) (&jobContext->vecOfThreads.at(i)); //TODO check
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
        sem_wait(&(jobContext->sem));
        sem_post(&(jobContext->sem)); // Check if correct
    }
    else{
        threadContext->globalJobContext->myState.stage = SHUFFLE_STAGE;
        threadContext->globalJobContext->myState.percentage = 0;
        threadContext->globalJobContext->numOfIntermediatePairs = 0;
        for (auto it : jobContext->vecOfThreads){
            threadContext->globalJobContext->numOfIntermediatePairs += it.threadIntermediateVec->size();
        }
        while (true){
            K2 *curLargestKey = findLargestKey(jobContext);
            if (curLargestKey == nullptr){
                threadContext->globalJobContext->myState.stage = REDUCE_STAGE;
                threadContext->globalJobContext->myState.percentage = 0.0;
                break;
            }
            IntermediateVec intermediateVecTemp;
            for (ThreadContext tc : jobContext->vecOfThreads){
                if (tc.threadIntermediateVec->empty()){
                    break;
                }
                IntermediatePair temp = tc.threadIntermediateVec->back();
                if (!(*temp.first < *curLargestKey) &&
                        !(*curLargestKey < *temp.first)){
                    intermediateVecTemp.push_back(temp);
                    tc.threadIntermediateVec->pop_back();
                }
            }
            jobContext->vecOfIntermediateVec->insert(jobContext->vecOfIntermediateVec->begin(),
                                                     intermediateVecTemp); // TODO: fix deep copy
            (*threadContext->globalJobContext->shuffleCompleted)++;
            //threadContext->globalJobContext->myState.percentage += ((((float)100.0)/(float)threadContext->globalJobContext->numOfIntermediatePairs)*(float)intermediateVecTemp.size());
            //(*(threadContext->globalJobContext->atomic_counter))++;
            //intermediateVecTemp.clear(); //TODO maybe
        }
        sem_post(&(jobContext->sem));
    }
}

void emit2 (K2* key, V2* value, void* context) {
    ThreadContext* threadContext = (ThreadContext*) context;
    //IntermediatePair* curPair = new IntermediatePair (key, value); //TODO: need to free?
    threadContext->threadIntermediateVec->push_back({key, value});
    //TODO maybe add mutex/barrie/sam
}

void emit3 (K3* key, V3* value, void* context) {
    ThreadContext* pContext = (ThreadContext*) context;

    pthread_mutex_lock(&pContext->globalJobContext->pthreadMutex);
    OutputPair newPair(key, value);
    auto it = std::lower_bound(pContext->globalJobContext->outputVec->begin(), pContext->globalJobContext->outputVec->end()
                               , newPair, compareOutputPair);
    pContext->globalJobContext->outputVec->insert(it, newPair); // TODO: check deep copy
    pthread_mutex_unlock(&pContext->globalJobContext->pthreadMutex);
    //pContext->mapReduceClient->reduce(pContext->threadIntermediateVec, pContext);
}

void reducePhase(ThreadContext *pContext);

void* mapWraper(void* arg){
    ThreadContext* threadContext = (ThreadContext*) arg;
    threadContext->globalJobContext->myState.stage = MAP_STAGE;
    while((threadContext->oldAtomicVal = (*threadContext->globalJobContext->atomic_counter)++) < threadContext->inputVec->size()) {
        threadContext->mapReduceClient->map(threadContext->inputVec->at(threadContext->oldAtomicVal).first,
                                            threadContext->inputVec->at(threadContext->oldAtomicVal).second,
                                            threadContext);
        (*threadContext->globalJobContext->mapCompleted)++;
        //threadContext->oldAtomicVal = (*(threadContext->globalJobContext->atomic_counter))++;
    }
    threadContext->globalJobContext->barrier->barrier(); //TODO: maybe to cancel
    sortIntermediateVec(threadContext);
    threadContext->globalJobContext->barrier->barrier();

    //end barrier

    insertIntermediateVecs(threadContext->globalJobContext, threadContext);

    //int x =3;
    //threadContext->globalJobContext->barrier->barrier(); //TODO check if needed
    //TODO: maybe need in loop
    reducePhase(threadContext);
    //threadContext->globalJobContext->barrier->barrier();
    //threadContext->globalJobContext->myState.percentage = 100.0;
    //TODO: maybe need barrier
    //pthread_mutex_unlock(&mtx);
    return 0;
}

void reducePhase(ThreadContext *pContext) {
    while (true) {
        pthread_mutex_lock(&pContext->globalJobContext->pthreadMutex);
        if (pContext->globalJobContext->vecOfIntermediateVec->empty()) {
            pthread_mutex_unlock(&pContext->globalJobContext->pthreadMutex);
            return;
        }
        //IntermediateVec temp = pContext->globalJobContext->vecOfIntermediateVec->back();
        /*IntermediateVec tempVec(pContext->globalJobContext->vecOfIntermediateVec->back().begin(),
                                pContext->globalJobContext->vecOfIntermediateVec->back().end());*/
        pContext->threadReduceIntermediateVec->assign(pContext->globalJobContext->vecOfIntermediateVec->back().begin(),
                                                pContext->globalJobContext->vecOfIntermediateVec->back().end()); // TODO check deep copy
        int sizeOfNewVec = pContext->threadReduceIntermediateVec->size();
        pContext->globalJobContext->vecOfIntermediateVec->pop_back();
        (*pContext->globalJobContext->reduceCompleted)+=sizeOfNewVec;
        /*pContext->globalJobContext->myState.percentage +=
                ((((float)sizeOfNewVec)/(float)pContext->globalJobContext->numOfIntermediatePairs)*100.0f);*/
        pthread_mutex_unlock(&pContext->globalJobContext->pthreadMutex);
        pContext->mapReduceClient->reduce(pContext->threadReduceIntermediateVec, pContext);
    }
}


JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel) {
    Barrier *barrier = new Barrier(multiThreadLevel);
    std::atomic<int> *atomic_counter = new std::atomic<int>(0);
    std::atomic<int> *mapCounter = new std::atomic<int>(0);
    std::atomic<int> *shuffleCounter = new std::atomic<int>(0);
    std::atomic<int> *reduceCounter = new std::atomic<int>(0);
    std::atomic<bool> *flagWait = new std::atomic<bool>(false);
    pthread_t *threads = new pthread_t[multiThreadLevel];
    JobContext *jobContext = new JobContext;
    if (pthread_mutex_init(&jobContext->pthreadMutex, nullptr) != 0)
    {
        return nullptr; //TODO: ERROR
    }
    if (sem_init(&jobContext->sem, 0, 1) != 0)
    {
        return nullptr; //TODO: ERROR
    }
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
    jobContext->myState.percentage = 0;
    for (int i = 0; i < multiThreadLevel; ++i) {
        ThreadContext *threadContext = new ThreadContext;
        threadContext->globalJobContext = jobContext;
        threadContext->ID = i;
        threadContext->thisThread = &threads[i];
        threadContext->mapReduceClient = &client;
        threadContext->inputVec = &inputVec;
        threadContext->threadIntermediateVec = new IntermediateVec;
        threadContext->threadReduceIntermediateVec = new IntermediateVec;
        jobContext->vecOfThreads.push_back(*threadContext);
    }
    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_create(&threads[i],
                       nullptr,
                       mapWraper,
                       &jobContext->vecOfThreads.at(i));
    }
/*
    for (int i = 0; i < multiThreadLevel; ++i){
        pthread_join(threads[i], nullptr);
    }
*/

    return static_cast<JobHandle>(jobContext);
}

void waitForJob(JobHandle job){
    JobContext* jobContext = (JobContext*) job;
  /*  if (jobContext->myState.stage != REDUCE_STAGE || *jobContext->reduceCompleted != jobContext->numOfIntermediatePairs){
        return;
    }*/
    if (*jobContext->flagWait){
        return;
    }
    //pthread_mutex_lock(&jobContext->pthreadMutex);
    *jobContext->flagWait = true;
    for (int i =0 ; i< jobContext->vecOfThreads.size(); i++){
        pthread_join(*jobContext->vecOfThreads.at(i).thisThread, nullptr);
    }
    //pthread_mutex_unlock(&jobContext->pthreadMutex);
}

void getJobState(JobHandle job, JobState* state){
    JobContext* jobContext = (JobContext*) job;
    state->stage = jobContext->myState.stage;
    float curPercentage = 0;
    switch (jobContext->myState.stage) {
        case MAP_STAGE:
            curPercentage = ((*jobContext->mapCompleted) / (float)jobContext->vecOfThreads.at(0).inputVec->size()) * 100;
            break;
        case SHUFFLE_STAGE:
            curPercentage = ((*jobContext->shuffleCompleted) / (float)jobContext->numOfIntermediatePairs) * 100;
            break;
        case REDUCE_STAGE:
            curPercentage = ((*jobContext->reduceCompleted) / (float)jobContext->numOfIntermediatePairs) * 100;
            break;
    }
    state->percentage = curPercentage;
}

void closeJobHandle(JobHandle job){
    JobContext* jobContext = (JobContext*) job;
    if (jobContext->myState.stage != REDUCE_STAGE || *jobContext->reduceCompleted != jobContext->numOfIntermediatePairs){
        waitForJob(job);
    }

    //TODO: maybe need to join here too.
    for (ThreadContext tc : jobContext->vecOfThreads) {
        delete tc.threadIntermediateVec;
    }
    delete jobContext->vecOfIntermediateVec;
    pthread_mutex_destroy(&jobContext->pthreadMutex);
    sem_destroy(&jobContext->sem);

    delete jobContext;

}
