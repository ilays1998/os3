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
    const InputVec* inputVec;
    IntermediateVec* threadIntermediateVec;
    JobContext *globalJobContext;

    //OutputVec* threadOutputVec;
};

struct JobContext {
    JobState myState;
    std::vector<ThreadContext> vecOfThreads;
    bool flagWait;
    ThreadContext *threadsContext;
    pthread_mutex_t pthreadMutex;
    sem_t sem;
    std::atomic<size_t>* atomic_counter;
    std::atomic<int>* atomicStage;
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
    ThreadContext *tc;
    K2* large = nullptr;
    for (int i = 0; i < jobContext->numOfThreads; i++){
        tc = (ThreadContext*) (jobContext->threadsContext + i*(sizeof(ThreadContext))); //TODO check
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
        threadContext->globalJobContext->myState.atomic = SHUFFLE_STAGE;
        threadContext->globalJobContext->myState.percentage = 0;
        threadContext->globalJobContext->atomic_counter->store(0);
        int numOfIntermediatePairs = 0;
        for (auto it : jobContext->vecOfThreads){
            numOfIntermediatePairs += it.threadIntermediateVec->size();
        }
        while (true){
            K2 *curLargestKey = findLargestKey(jobContext);
            if (curLargestKey == nullptr){
                break;
            }
            IntermediateVec intermediateVecTemp;
            for (ThreadContext tc : jobContext->vecOfThreads){
                IntermediatePair temp = tc.threadIntermediateVec->back();
                if (!(temp.first < curLargestKey) &&
                        !(curLargestKey < temp.first)){

                    intermediateVecTemp.push_back(temp);
                    tc.threadIntermediateVec->pop_back();
                }
            }
            jobContext->vecOfIntermediateVec->insert(jobContext->vecOfIntermediateVec->begin(),
                                                     intermediateVecTemp);
            threadContext->globalJobContext->myState.percentage += ((((float)100.0)/(float)numOfIntermediatePairs)*(float)intermediateVecTemp.size());
            //(*(threadContext->globalJobContext->atomic_counter))++;
            //intermediateVecTemp.clear(); //TODO maybe
        }

        sem_post(&(jobContext->sem));
    }
}

void emit2 (K2* key, V2* value, void* context) {
    ThreadContext* threadContext = (ThreadContext*) context;
    IntermediatePair* curPair = new IntermediatePair (key, value); //TODO: need to free?
    threadContext->threadIntermediateVec->push_back(*curPair);
    //TODO maybe add mutex/barrie/sam
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
    pthread_mutex_lock(&threadContext->globalJobContext->pthreadMutex);
    size_t i = (*(threadContext->globalJobContext->atomic_counter))++;
    pthread_mutex_unlock(&threadContext->globalJobContext->pthreadMutex);
    while(i < threadContext->inputVec->size()) {
        threadContext->mapReduceClient->map(threadContext->inputVec->at(i).first,
                                            threadContext->inputVec->at(i).second,
                                            threadContext);
        pthread_mutex_lock(&threadContext->globalJobContext->pthreadMutex);
        threadContext->globalJobContext->atomicStage->store(1);
        threadContext->globalJobContext->myState.percentage += (((float)100.0)/(float)threadContext->inputVec->size());
        i = (*(threadContext->globalJobContext->atomic_counter))++;
        pthread_mutex_unlock(&threadContext->globalJobContext->pthreadMutex);
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
    std::atomic<size_t> atomic_counter(0);
    ThreadContext allThreadsContext[multiThreadLevel];
    pthread_t threads[multiThreadLevel];
    JobContext *jobContext = new JobContext;
    if (pthread_mutex_init(&jobContext->pthreadMutex, NULL) != 0)
    {
        return NULL; //TODO: ERROR
    }
    jobContext->barrier = &barrier;
    jobContext->flagWait = false;
    jobContext->outputVec = &outputVec;
    jobContext->threadsContext = allThreadsContext;
    jobContext->atomic_counter = &atomic_counter;
    jobContext->vecOfIntermediateVec = new VecOfIntermediateVec ;
    jobContext->numOfThreads = multiThreadLevel;
    //int pairsForThread = inputVec.size()/multiThreadLevel;
    *(jobContext->atomicStage) = 0;
    jobContext->myState.atomic = static_cast<stage_t>(jobContext->atomicStage->load());
    jobContext->myState.percentage = 0;
    for (int i = 0; i < multiThreadLevel; i++) {
        //ThreadContext* threadContext = new ThreadContext;
        allThreadsContext[i].globalJobContext = jobContext;
        allThreadsContext[i].ID = i;
        allThreadsContext[i].thisThread = threads[i];
        allThreadsContext[i].mapReduceClient = &client;
        allThreadsContext[i].inputVec = &inputVec;
        allThreadsContext[i].threadIntermediateVec = new IntermediateVec;
        jobContext->vecOfThreads.push_back(allThreadsContext[i]);

        pthread_create(threads + i, NULL, mapWraper, &allThreadsContext[i]);
    }
/*    for (int i = 0; i < multiThreadLevel; i++){
        pthread_join(allThreadsContext[i].thisThread, NULL);
    }*/

    //
    return static_cast<JobHandle>(jobContext);
}

void waitForJob(JobHandle job){
    JobContext* jobContext = (JobContext*) job;
    if (jobContext->flagWait)
        return;
    jobContext->flagWait = true;
    for (ThreadContext tc : jobContext->vecOfThreads){
        pthread_join(tc.thisThread, NULL);
    }
}

void getJobState(JobHandle job, JobState* state){
    JobContext* jobContext = (JobContext*) job;
    state->atomic = jobContext->myState.atomic;
    state->percentage = jobContext->myState.percentage;
}

void closeJobHandle(JobHandle job){
    JobContext* jobContext = (JobContext*) job;
    if (jobContext->myState.atomic != REDUCE_STAGE || jobContext->myState.percentage != 100.0){
        waitForJob(job);
    }

    for (ThreadContext tc : jobContext->vecOfThreads) {
        delete tc.threadIntermediateVec;
    }
    delete jobContext->vecOfIntermediateVec;
    pthread_mutex_destroy(&jobContext->pthreadMutex);
    sem_destroy(&jobContext->sem);

    delete jobContext;

}
