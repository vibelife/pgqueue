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
        // clear up the requests
        while (!requests.empty()) {
            while (!requests.empty()) {
                // printf("Clearing out [requests.size() = %li]\n", requests.size());
                std::this_thread::sleep_for(100ms);
                aRequests.test_and_set();
                aRequests.notify_one();
            }
        }


        if (!responses.empty()) {
            while (!responses.empty()) {
                // printf("Clearing out [responses.size() = %li]\n", responses.size());
                std::this_thread::sleep_for(100ms);
                aResponses.test_and_set();
                aResponses.notify_one();
            }
        }

        isRunning.clear();

        // this will force the background wait loops to exit
        aRequests.test_and_set();
        aRequests.notify_one();
        aResponses.test_and_set();
        aResponses.notify_one();
    }
};

#endif //PGQUEUE_PGQUERYPROCESSINGSTATE_HPP
