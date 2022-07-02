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

#undef strerror

class PGQueryProcessor {
private:
    rigtorp::MPMCQueue<PGQueryRequest*> requests;
    rigtorp::MPMCQueue<PGQueryResponse*> responses;
    PGConnectionPool* pool{};
    char const* connString;
    std::thread responseHandlerThread;
    bool requestsReady{};
    std::atomic<bool> isRunning{true};
    std::condition_variable cv;
    std::mutex mRequests;
    boost::asio::thread_pool threadPool;
private:
    static void printError(const char* errMsg, int err) {
        std::cerr << "[Error] " << errMsg << ": " << strerror(err) << "\n" << std::flush;
    }

    /**
     * Adds an item to the queue
     * @return
     */
    void pushRequest(PGQueryRequest* request) {
        requests.push(request);

        {
            std::lock_guard lk(mRequests);
            if (requestsReady) {
                return;
            }

            requestsReady = true;
        }
        cv.notify_one();
    }
public:
    PGQueryProcessor(char const* connectionString, size_t depth = 128, size_t nbThreads = 4)
        :connString(connectionString), requests(depth), responses(depth), threadPool(nbThreads)
    {}

    ~PGQueryProcessor() {
        responseHandlerThread.join();
        delete pool;
    }

    /**
     * Process requests in a background thread
     */
    void go(unsigned connPoolSize = 4) {
        pool = new PGConnectionPool{};
        pool->go(connString, connPoolSize, requestsReady, mRequests, cv, requests, responses);
        responseHandlerThread = std::thread([&] {
            while (isRunning) {
                PGQueryResponse* response{};
                responses.pop(response);
                // printf("(1) callback %ld\n", response->id);
                auto cb = std::move(response->callback);
                auto resultSet = std::move(response->resultSet);
                delete response;

                // there may not be a callback
                if (cb != nullptr) {
                    boost::asio::post(threadPool, [cb = std::move(cb), resultSet = std::move(resultSet)] () mutable {
                        // printf("(2) callback %ld\n", response);
                        cb(std::move(resultSet));
                    });
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
