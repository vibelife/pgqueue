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
    void connectAll(struct io_uring *ring, char const* connectionString, unsigned int nbConnections) {
        for (int i = 0; i < nbConnections; i += 1) {
            auto conn = new PGConnection();
            if (conn->connect(connectionString) == PGConnection::OK) {
                conn->setup(epfd, ring);
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
        return std::all_of(connections.cbegin(), connections.cend(), isReadyFn);
    }

    /**
     * Process requests
     */
    void go(char const* connectionString, unsigned int nbConnections, bool &requestsReady, std::mutex &m, std::condition_variable &cv, rigtorp::MPMCQueue<PGQueryRequest*> &requests, rigtorp::MPMCQueue<PGQueryResponse*> &responses) {
        thrd = std::thread([this, connectionString, nbConnections, &requests, &responses, &m, &cv, &requestsReady] {
            // set up liburing
            struct io_uring ring{};
            int ret = io_uring_queue_init(QUEUE_DEPTH, &ring, 0);
            if (ret < 0) {
                printError("io_uring_queue_init: ", ret);
                exit(EXIT_FAILURE);
                return;
            }

            std::vector<io_uring_cqe*> cqes{};
            cqes.reserve(NB_EVENTS);
            cqes.insert(cqes.begin(), NB_EVENTS, nullptr);

            // 100ms timespec
            __kernel_timespec ts{.tv_sec = 0, .tv_nsec = 100000000};

            epfd = epoll_create1(0);
            if (epfd < 0) {
                printError("epoll_create1: ", epfd);
            }

            // create multiple connections to the database
            connectAll(&ring, connectionString, nbConnections);

            while (isRunning) {
                // wait for another thread to update [canProcess]
                {
                    std::unique_lock lk(m);
                    cv.wait(lk, [&requestsReady] { return requestsReady; });
                }

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
                    io_uring_wait_cqe_timeout(&ring, cqes.data(), &ts);

                    for (io_uring_cqe*& cqe: cqes) {
                        if (cqe != nullptr) {
                            if (cqe->res == -EAGAIN) {
                                io_uring_cqe_seen(&ring, cqe);
                                continue;
                            }

                            auto ptr = static_cast<PGConnection*>(io_uring_cqe_get_data(cqe));
                            if (ptr != nullptr) {
                                ptr->doNextStep(cqe->res, responses);
                            }
                        }
                        io_uring_cqe_seen(&ring, cqe);
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
        });
    }
};

#endif //PGQUEUE_PGCONNECTIONPOOL_HPP
