//
// Created by anewman on 2022-06-28.
//

#ifndef PGQUEUE_PGQUERYPROCESSOR_HPP
#define PGQUEUE_PGQUERYPROCESSOR_HPP

#include <sys/epoll.h>
#include <thread>
#include <mutex>
#include <condition_variable>

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
    std::thread thread;
    bool requestsReady{};
    std::condition_variable cv;
    std::mutex m;
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
            std::lock_guard lk(m);
            if (requestsReady) {
                return;
            }

            requestsReady = true;
        }
        cv.notify_one();
    }
public:
    PGQueryProcessor(char const* connectionString, size_t depth = 128)
        :connString(connectionString), requests(depth), responses(depth) {}

    ~PGQueryProcessor() {
        thread.join();
        delete pool;
    }

    /**
     * Process requests
     */
    void go() {
        pool = new PGConnectionPool{};
        pool->go(connString, 2, requestsReady, m, cv, requests, responses);
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
