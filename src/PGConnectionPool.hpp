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

#undef strerror

class PGConnectionPool {
public:
    static constexpr auto QUEUE_DEPTH = 128;
    static constexpr unsigned int NB_EVENTS = 2;
    static constexpr unsigned int NB_CONNECTIONS = 2;
private:
    std::thread thrd;
    bool isRunning = true;
    int epfd{};
    std::unordered_map<int, PGConnection*> connections{};
    static constexpr auto isReadyFn = [](auto const& p) { return p.second->isReady(); };
    static constexpr auto isDoneFn = [](auto const& p) { return p.second->isDone(); };
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
     */
    void connectAllURing(struct io_uring *ring, char const* connectionString, unsigned int nbConnections) {
        for (int i = 0; i < nbConnections; i += 1) {
            auto conn = new PGConnection();
            if (conn->connect(connectionString) == PGConnection::PGConnectionResult_Ok) {
                conn->setupURing(epfd, ring);
                connections.emplace(conn->fd(), conn);
            } else {
                // if any connection fails, then we quit.
                exit(EXIT_FAILURE);
            }
        }
        std::cout << "Connection Pool: " << nbConnections << " connection(s) established" << "\n";
    }

    /**
     * Create multiple connections to the database
     * @param ring
     * @param connectionString
     * @param nbConnections
     */
    void connectAllEPoll(char const* connectionString, unsigned int nbConnections) {
        for (int i = 0; i < nbConnections; i += 1) {
            auto conn = new PGConnection();
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
        isRunning = false;
        close(epfd);
        thrd.join();
        while (!connections.empty()) {
            delete connections.begin()->second;
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
     * @param requestsReady
     * @param m
     * @param cv
     * @param requests
     * @param responses
     */
    void runWithEPoll(char const* connectionString, unsigned int nbConnections, bool &requestsReady, std::mutex &m, std::condition_variable &cv, rigtorp::MPMCQueue<PGQueryRequest*> &requests, rigtorp::MPMCQueue<PGQueryResponse*> &responses) {
        // create multiple connections to the database
        connectAllEPoll(connectionString, nbConnections);

        struct epoll_event events[NB_EVENTS];
        std::unique_lock lock{m, std::defer_lock};

        while (isRunning) {
            // wait for another thread to update [canProcess]
            cv.wait(lock, [&requestsReady] { return requestsReady; });

            drainQueue:
            // Drain the queue as much as we can
            while (!requests.empty() && hasReadyConnections()) {
                PGQueryRequest* request{};
                requests.pop(request);
                submit(request);
            }

            // check if there are more requests than available DB connections
            bool hasMoreRequests = !requests.empty();

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
                    connections[events[i].data.fd]->doNextStep(1, responses);
                }
            }


            if (hasMoreRequests) {
                // to get here means there were more requests than available connections
                // eventually the request queue will hit its cap and block the thread trying to add more.
                goto drainQueue;
            } else {
                std::unique_lock lk(m);
                requestsReady = false;
            }
        }
    }

    /**
     * Process queries in a background thread
     */
    void go(char const* connectionString, unsigned int nbConnections, bool &requestsReady, std::mutex &m, std::condition_variable &cv, rigtorp::MPMCQueue<PGQueryRequest*> &requests, rigtorp::MPMCQueue<PGQueryResponse*> &responses) {
        thrd = std::thread([this, connectionString, nbConnections, &requests, &responses, &m, &cv, &requestsReady] {
            epfd = epoll_create1(0);
            if (epfd < 0) {
                printError("epoll_create1: ", epfd);
                exit(EXIT_FAILURE);
            }

            runWithEPoll(connectionString, nbConnections, requestsReady, m, cv, requests, responses);
        });
    }
};

#endif //PGQUEUE_PGCONNECTIONPOOL_HPP
