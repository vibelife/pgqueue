#ifndef PGQUEUE_PGCONNECTIONPOOL_HPP
#define PGQUEUE_PGCONNECTIONPOOL_HPP

#include <thread>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <unordered_map>
#include <cstring>

#include "MPMCQueue.hpp"
#include "PGConnection.hpp"

#include "PGQueryProcessingState.hpp"

#undef strerror

class PGConnectionPool {
private:
    static constexpr unsigned int NB_EVENTS = 2;
    static constexpr auto isReadyFn = [](auto const& p) { return p.second.isReady(); };
    static constexpr auto isDoneFn = [](auto const& p) { return p.second.isDone(); };
    std::jthread thrd;
    int epfd{};
    std::unordered_map<int, PGConnection> connections{};
private:
    static void printError(const char* errMsg, int err) {
        printf("[Error] %s: %s\n", errMsg, strerror(err));
    }

    static void printError(char const* msg) {
        printf("%s\n", msg);
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
            auto conn = PGConnection{nbQueriesPerConnection};
            if (conn.connect(connectionString) == PGConnection::PGConnectionResult_Ok) {
                conn.setupEPoll(epfd);
                connections.emplace(conn.fd(), std::move(conn));
            } else {
                // if any connection fails, then we quit.
                exit(EXIT_FAILURE);
            }
        }
        printf("Connection Pool: %i  connection(s) established\n", nbConnections);
    }
public:
    ~PGConnectionPool() {
        close(epfd);
        connections.clear();
    }

    /**
     * Submits the query on the first available connection
     * @param request
     */
    void submit(PGQueryRequest &&request) {
        for (auto &[fd, conn]: connections) {
            if (conn.isReady()) {
                conn.sendRequest(std::move(request));
                break;
            }
        }
    }

    /**
     * Returns true if any connection is ready to push
     * @return
     */
    bool hasReadyConnections() {
        return std::any_of(connections.cbegin(), connections.cend(), isReadyFn);
    }

    /**
     * Returns true if all connections are ready to push
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
     * @param state
     */
    void runWithEPoll(char const* connectionString, unsigned int nbConnections, unsigned int nbQueriesPerConnection, PGQueryProcessingState &state) {
        // create multiple connections to the database
        connectAllEPoll(connectionString, nbConnections, nbQueriesPerConnection);

        struct epoll_event events[NB_EVENTS];

        while (state.isRunning.test()) {
            // wait for another thread to alert us when a query is submitted
            state.aRequests.wait(false);

            drainQueue:
            // Drain the queue as much as we can
            while (!state.requests.empty() && hasReadyConnections()) {
                PGQueryRequest request;
                state.requests.pop(request);
                submit(std::move(request));
            }

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
                    connections[events[i].data.fd].doNextStep(1, state.responses, state);
                }
            }


            if (!state.requests.empty()) {
                // To get here means there were more requests than available connections, or more requests came in.
                // Eventually the request queue will hit its cap and block the thread trying to add more.
                goto drainQueue;
            } else {
                state.aRequests.clear();
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
        thrd = std::jthread([this, connectionString, nbConnections, nbQueriesPerConnection, &state] {
            epfd = epoll_create1(0);
            if (epfd < 0) {
                printError("epoll_create1: ", epfd);
                exit(EXIT_FAILURE);
            }

            runWithEPoll(connectionString, nbConnections, nbQueriesPerConnection, state);
        });

        using namespace std::chrono_literals;
        std::this_thread::sleep_for(1s);
    }
};

#endif //PGQUEUE_PGCONNECTIONPOOL_HPP
