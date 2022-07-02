//
// Created by anewman on 2022-06-28.
//

#ifndef PGQUEUE_PGQUERYPROCESSOR_HPP
#define PGQUEUE_PGQUERYPROCESSOR_HPP

#include <sys/epoll.h>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <boost/asio.hpp>

#include "MPMCQueue.hpp"
#include "PGQueryStructures.hpp"
#include "PGEventHandler.hpp"
#include "PGConnectionPool.hpp"
#include "PGQueryProcessingState.hpp"

#undef strerror

class PGQueryProcessor {
private:
    //rigtorp::MPMCQueue<PGQueryRequest*> requests;
    //rigtorp::MPMCQueue<PGQueryResponse*> responses;
    PGConnectionPool* pool{};
    char const* connString;
    boost::asio::thread_pool responseThreadPool;
    unsigned int nbConnectionsInPool{};
    unsigned int nbQueriesPerConnection{};
    std::thread responseHandlerThread;

    //std::condition_variable cvRequests;
    //std::mutex mRequests;
    //bool hasRequestsToProcess{};
    PGQueryProcessingState state;
private:
    static void printError(const char* errMsg, int err) {
        std::cerr << "[Error] " << errMsg << ": " << strerror(err) << "\n" << std::flush;
    }

    /**
     * Adds an item to the queue
     * @return
     */
    void pushRequest(PGQueryRequest* request) {
        state.requests.push(request);

        {
            std::lock_guard lk(state.mRequests);
            if (state.hasRequestsToProcess) {
                return;
            }

            state.hasRequestsToProcess = true;
        }
        state.cvRequests.notify_one();
    }
public:
    explicit PGQueryProcessor(
            char const* connectionString,
            unsigned int nbConnectionsInPool = 4,
            unsigned int nbQueriesPerConnection = 4,
            size_t maxQueueDepth = 128,
            size_t nbThreadsInResponseCallbackPool = 4
        )
        : state(maxQueueDepth), connString(connectionString), responseThreadPool(nbThreadsInResponseCallbackPool), nbConnectionsInPool(nbConnectionsInPool), nbQueriesPerConnection(nbQueriesPerConnection)
    {}

    ~PGQueryProcessor() {
        responseHandlerThread.join();
        delete pool;
    }

    /**
     * Process requests in a background thread
     */
    void go() {
        pool = new PGConnectionPool{};
        pool->go(connString, nbConnectionsInPool, nbQueriesPerConnection, state);
        responseHandlerThread = std::thread([&] {
            while (state.isRunning) {
                std::unique_lock lock{state.mResponses};
                state.cvResponses.wait(lock, [&] { return state.hasResponsesToProcess; });
                lock.unlock();

                while (!state.responses.empty()) {
                    PGQueryResponse* response{};
                    state.responses.pop(response);
                    auto cb = std::move(response->callback);
                    auto resultSet = std::move(response->resultSet);
                    delete response;

                    // there may not be a callback
                    if (cb != nullptr) {
                        boost::asio::post(responseThreadPool, [cb = std::move(cb), resultSet = std::move(resultSet)]() mutable {
                            cb(std::move(resultSet));
                        });
                    }
                }

                {
                    std::lock_guard lk(state.mResponses);
                    state.hasResponsesToProcess = false;
                }
            }
        });
    }

    /**
     * Pushes a query onto the queue
     * @param queryParams - The SQL query params
     * @param callback - If this is null it is like a fire-and-forget.
     * @return
     */
    void push(PGQueryParams* queryParams, std::function<void(PGResultSet&&)>&& callback = nullptr) {
        pushRequest(new PGQueryRequest{false, queryParams, std::move(callback)});
    }
};

#endif //PGQUEUE_PGQUERYPROCESSOR_HPP
