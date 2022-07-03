//
// Created by anewman on 2022-06-29.
//

#ifndef PGQUEUE_PGCONNECTIONPOOL_HPP
#define PGQUEUE_PGCONNECTIONPOOL_HPP

#include <thread>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <unordered_map>
#include <liburing.h>
#include <cstring>

#include "MPMCQueue.hpp"
#include "PGConnection.hpp"

#include "PGQueryProcessingState.hpp"

#undef strerror

class PGConnectionPool {
private:
    static constexpr unsigned int NB_EVENTS = 2;
    static constexpr auto isReadyFn = [](auto const& p) { return p.second->isReady(); };
    static constexpr auto isDoneFn = [](auto const& p) { return p.second->isDone(); };
    std::thread thrd;
    int epfd{};
    std::unordered_map<int, PGConnection*> connections{};
private:
    static void printError(const char* errMsg, int err) {
        std::cerr << "[Error] " << errMsg << ": " << strerror(err) << "\n" << std::flush;
    }

    static void printError(char const* msg) {
        std::cerr << msg << std::endl << std::flush;
    }

    /**
     * Create multiple connections to the database
     * @param ring
     * @param connectionString
     * @param nbConnections
     * @param nbQueriesPerConnection
     */
    void connectAllEPoll(char const* connectionString, unsigned int nbConnections, unsigned int nbQueriesPerConnection) {
        for (int i = 0; i < nbConnections; i += 1) {
            auto conn = new PGConnection(nbQueriesPerConnection);
            if (conn->connect(connectionString) == PGConnection::PGConnectionResult_Ok) {
                conn->setupEPoll(epfd);
                connections.emplace(conn->fd(), conn);
            } else {
                // if any connection fails, then we quit.
                exit(EXIT_FAILURE);
            }
        }
        std::cout << "Connection Pool: " << nbConnections << " connection(s) established" << "\n";
    }
public:
    ~PGConnectionPool() {
        close(epfd);
        thrd.join();
        for (auto& p : connections) {
            delete p.second;
        }
        connections.clear();
    }

    /**
     * Submits the query on the first available connection
     * @param request
     */
    void submit(PGQueryRequest* request) {
        for (auto &[fd, conn]: connections) {
            if (conn->sendRequestIfReady(request)) {
                break;
            }
        }
    }

    /**
     * Returns true if any connection is ready to process
     * @return
     */
    bool hasReadyConnections() {
        return std::any_of(connections.cbegin(), connections.cend(), isReadyFn);
    }

    /**
     * Returns true if all connections are ready to process
     * @return
     */
    bool isDone() {
        return std::all_of(connections.cbegin(), connections.cend(), isDoneFn);
    }

    /**
     * Handles sending queries with epoll
     * @param connectionString
     * @param nbConnections
     * @param nbQueriesPerConnection
     * @param hasRequestsToProcess
     * @param m
     * @param cvRequests
     * @param requests
     * @param responses
     */
    void runWithEPoll(char const* connectionString, unsigned int nbConnections, unsigned int nbQueriesPerConnection, PGQueryProcessingState &state) {
        // create multiple connections to the database
        connectAllEPoll(connectionString, nbConnections, nbQueriesPerConnection);

        struct epoll_event events[NB_EVENTS];

        while (state.isRunning) {
            // wait for another thread to alert us when a query is submitted
            std::unique_lock lock{state.mRequests};
            state.cvRequests.wait(lock, [&] { return state.requestsLockState; });
            lock.unlock();

            drainQueue:
            // Drain the queue as much as we can
            while (!state.requests.empty() && hasReadyConnections()) {
                PGQueryRequest* request{};
                state.requests.pop(request);
                submit(request);
            }

            // check if there are more requests than available DB connections
            bool hasMoreRequests = !state.requests.empty();

            while (!isDone()) {
                int nbFds = epoll_wait(epfd, events, NB_EVENTS, -1);
                if (nbFds == -1) {
                    if (errno == EINTR) {
                        continue;
                    }
                    printError("epoll_wait");
                    exit(EXIT_FAILURE);
                }

                for (int i = 0; i < nbFds; i += 1) {
                    connections[events[i].data.fd]->doNextStep(1, state.responses, state);
                }
            }


            if (hasMoreRequests) {
                // to get here means there were more requests than available connections
                // eventually the request queue will hit its cap and block the thread trying to add more.
                goto drainQueue;
            } else {
                std::lock_guard lk(state.mRequests);
                state.requestsLockState = PGQueryProcessingState::LockStates_WAIT;
            }
        }
    }

    /**
     * Process queries in a background thread
     * @param connectionString
     * @param nbConnections
     * @param nbQueriesPerConnection
     * @param state
     */
    void go(char const* connectionString, unsigned int nbConnections, unsigned int nbQueriesPerConnection, PGQueryProcessingState &state) {
        thrd = std::thread([this, connectionString, nbConnections, nbQueriesPerConnection, &state] {
            epfd = epoll_create1(0);
            if (epfd < 0) {
                printError("epoll_create1: ", epfd);
                exit(EXIT_FAILURE);
            }

            runWithEPoll(connectionString, nbConnections, nbQueriesPerConnection, state);
        });
    }
};

#endif //PGQUEUE_PGCONNECTIONPOOL_HPP
