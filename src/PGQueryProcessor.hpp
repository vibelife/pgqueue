#ifndef PGQUEUE_PGQUERYPROCESSOR_HPP
#define PGQUEUE_PGQUERYPROCESSOR_HPP

#include <sys/epoll.h>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <boost/asio.hpp>

#include "MPMCQueue.hpp"
#include "PGQueryStructures.hpp"
#include "PGConnectionPool.hpp"
#include "PGQueryProcessingState.hpp"

#undef strerror

class PGQueryProcessor {
private:
    PGConnectionPool pool{};
    char const* connString;
    boost::asio::thread_pool responseThreadPool;
    unsigned int nbConnectionsInPool{};
    unsigned int nbQueriesPerConnection{};
    std::jthread responseHandlerThread;
    PGQueryProcessingState state;
private:
    static void printError(const char* errMsg, int err) {
        printf("[Error] %s: %s\n", errMsg, strerror(err));
    }

    /**
     * Adds an item to the queue
     * @return
     */
    void pushRequest(PGQueryRequest* request) {
        state.requests.push(request);

        {
            std::lock_guard lk(state.mRequests);
            if (state.requestsLockState == PGQueryProcessingState::LockStates_GO) {
                return;
            }

            state.requestsLockState = PGQueryProcessingState::LockStates_GO;
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
        state.cleanUp();
    }

    /**
     * Returns a new instance of a query processor. In most applications the default parameter values are enough, so
     * you'll only have to pass in the connection string, but if you need more performance start by increasing the 2nd param [nbConnectionsInPool], but
     * don't increase it above the maximum number of connections to your PostgreSQL version.
     * @param connectionString Can be a Unix Domain Socket for a boost in performance
     * @param nbConnectionsInPool Should not exceed the max number of connections to your PostgreSQL install, also should not exceed the number of cores on your CPU.
     * @param nbQueriesPerConnection Specifies how many queries that are concurrently sent over the same connection. The default param value will be enough in most cases.
     * @param maxQueueDepth Specifies how many pending queries are allowed. The default param value will be enough in most cases.
     * @param nbThreadsInResponseCallbackPool Specifies how many threads are used in the callback thread pool. The default param value will be enough in most cases.
     * @return
     */
    static PGQueryProcessor* createInstance(
            char const* connectionString,
            unsigned int nbConnectionsInPool = 4,
            unsigned int nbQueriesPerConnection = 4,
            size_t maxQueueDepth = 128,
            size_t nbThreadsInResponseCallbackPool = 4
    ) {
        auto retVal = new PGQueryProcessor(connectionString, nbConnectionsInPool, nbQueriesPerConnection, maxQueueDepth, nbThreadsInResponseCallbackPool);
        retVal->go();
        return retVal;
    }

    /**
     * Connects to the database, and starts the request processor in a background thread.
     */
    void go() {
        pool.go(connString, nbConnectionsInPool, nbQueriesPerConnection, state);
        responseHandlerThread = std::jthread([&] {
            while (state.isRunning.test()) {
                std::unique_lock lock{state.mResponses};
                state.cvResponses.wait(lock, [&] { return state.responsesLockState; });
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
                    state.responsesLockState = PGQueryProcessingState::LockStates_WAIT;
                }
            }
        });
    }

    /**
     * Pushes a query onto the queue
     * @param q - The SQL query
     * @param callback - If this is null it is like a fire-and-forget.
     * @return
     */
    void push(std::string&& q, std::function<void(PGResultSet&&)>&& callback = nullptr) {
        if (state.isRunning.test()) {
            pushRequest(new PGQueryRequest{false, PGQueryParams::Builder::create(std::move(q)).build(), std::move(callback)});
        }
    }

//    /**
//     * Pushes a query onto the queue
//     * @param q - The SQL query
//     * @param nbChars - The length of the SQL query
//     * @param callback - If this is null it is like a fire-and-forget.
//     * @return
//     */
//    void push(char const* q, size_t nbChars, std::function<void(PGResultSet&&)>&& callback = nullptr) {
//        if (state.isRunning.test()) {
//            pushRequest(new PGQueryRequest{false, PGQueryParams::Builder::create(q, nbChars).build(), std::move(callback)});
//        }
//    }

    /**
     * Pushes a query onto the queue
     * @param queryParams - The SQL query params
     * @param callback - If this is null it is like a fire-and-forget.
     * @return
     */
    void push(PGQueryParams* queryParams, std::function<void(PGResultSet&&)>&& callback = nullptr) {
        if (state.isRunning.test() && queryParams != nullptr) {
            pushRequest(new PGQueryRequest{false, queryParams, std::move(callback)});
        }
    }
};

#endif //PGQUEUE_PGQUERYPROCESSOR_HPP
