//
// Created by anewman on 2022-07-02.
//

#ifndef PGQUEUE_PGQUERYPROCESSINGSTATE_HPP
#define PGQUEUE_PGQUERYPROCESSINGSTATE_HPP

#include <thread>
#include <mutex>
#include <condition_variable>

#include "MPMCQueue.hpp"
#include "PGQueryStructures.hpp"

struct PGQueryProcessingState {
    enum LockStates {
        LockStates_WAIT = 0,
        LockStates_GO,
        LockStates_KILL,
    };

    std::atomic_flag isRunning{true};

    std::condition_variable cvRequests{};
    std::mutex mRequests{};
    // bool hasRequestsToProcess{false};
    LockStates requestsLockState{LockStates_WAIT};
    rigtorp::MPMCQueue<PGQueryRequest*> requests;

    std::condition_variable cvResponses{};
    std::mutex mResponses{};
    // bool hasResponsesToProcess{false};
    LockStates responsesLockState{LockStates_WAIT};
    rigtorp::MPMCQueue<PGQueryResponse*> responses;

    explicit PGQueryProcessingState(size_t queueDepths)
            :requests(queueDepths), responses(queueDepths)
    {}

    void cleanUp() {
        using namespace std::chrono_literals;

        isRunning.clear();
        // clear up the requests
        while (!requests.empty()) {
            PGQueryRequest *ptr{};
            requests.pop(ptr);
            delete ptr;
        }

        {
            std::lock_guard lk{mRequests};
            requestsLockState = LockStates_KILL;
        }
        cvRequests.notify_one();

        {
            std::lock_guard lk{mResponses};
            responsesLockState = LockStates_KILL;
        }
        cvResponses.notify_one();

        while (!requests.empty() && !responses.empty()) {
            printf("Clearing out [requests.size() = %li] [responses.size() = %li]\n", requests.size(), responses.size());
            std::this_thread::sleep_for(100ms);
        }
    }
};

#endif //PGQUEUE_PGQUERYPROCESSINGSTATE_HPP
