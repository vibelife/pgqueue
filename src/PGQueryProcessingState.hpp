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
    std::atomic<bool> isRunning{true};

    std::condition_variable cvRequests{};
    std::mutex mRequests{};
    bool hasRequestsToProcess{false};
    rigtorp::MPMCQueue<PGQueryRequest*> requests;

    std::condition_variable cvResponses{};
    std::mutex mResponses{};
    bool hasResponsesToProcess{false};
    rigtorp::MPMCQueue<PGQueryResponse*> responses;

    explicit PGQueryProcessingState(size_t queueDepths)
        :requests(queueDepths), responses(queueDepths)
    {}
};

#endif //PGQUEUE_PGQUERYPROCESSINGSTATE_HPP
