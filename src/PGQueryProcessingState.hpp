#ifndef PGQUEUE_PGQUERYPROCESSINGSTATE_HPP
#define PGQUEUE_PGQUERYPROCESSINGSTATE_HPP

#include <thread>
#include <mutex>
#include <condition_variable>

#include "MPMCQueue.hpp"
#include "PGQueryStructures.hpp"

struct PGQueryProcessingState {
    std::atomic_flag isRunning{true};

    rigtorp::MPMCQueue<PGQueryRequest> requests;
    std::atomic_flag aRequests;

    rigtorp::MPMCQueue<PGQueryResponse> responses;
    std::atomic_flag aResponses;

    explicit PGQueryProcessingState(size_t queueDepths)
            :requests(queueDepths), responses(queueDepths)
    {}

    void cleanUp() {
        using namespace std::chrono_literals;

        isRunning.clear();
        // clear up the requests
        while (!requests.empty()) {
            PGQueryRequest ptr;
            requests.pop(ptr);
        }

        // this will force the background wait loops to exit
        aRequests.test_and_set();
        aRequests.notify_one();
        aResponses.test_and_set();
        aResponses.notify_one();

        while (!requests.empty() && !responses.empty()) {
            printf("Clearing out [requests.size() = %li] [responses.size() = %li]\n", requests.size(), responses.size());
            std::this_thread::sleep_for(100ms);
        }
    }
};

#endif //PGQUEUE_PGQUERYPROCESSINGSTATE_HPP
