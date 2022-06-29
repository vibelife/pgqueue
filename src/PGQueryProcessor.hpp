//
// Created by anewman on 2022-06-28.
//

#ifndef PGQUEUE_PGQUERYPROCESSOR_HPP
#define PGQUEUE_PGQUERYPROCESSOR_HPP

#include <liburing.h>
#include <thread>
#include <sys/epoll.h>
#include <fcntl.h>
#include <sys/poll.h>

#include "MPMCQueue.hpp"
#include "PGQueryStructures.hpp"
#include "PGEventHandler.hpp"
#include "PGConnection.hpp"
#include "PGRequestWriter.hpp"

#undef strerror

class PGQueryProcessor {
private:
    static constexpr auto QUEUE_DEPTH = 128;
    static constexpr unsigned int NB_EVENTS = 2;
    static constexpr unsigned int NB_CONNECTIONS = 2;
    bool isRunning = true;
    rigtorp::MPMCQueue<PGQueryRequest*> requests{24};
    std::thread thrd;
    char const* connString;
    static void printError(const char* errMsg, int err) {
        std::cerr << "[Error] " << errMsg << ": " << strerror(err) << "\n" << std::flush;
    }

    /**
     * Adds an item to the queue
     * @return
     */
    void pushRequest(PGQueryRequest* request) {
        requests.push(request);
    }

    void readRequest(io_uring *ring, int res) {

    }

    /**
     * Process requests
     */
    void processRequests() {
        thrd = std::thread([this] {
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

            size_t nbProcessing{}, nbComplete{};
            // 1 second timespec
            __kernel_timespec ts{.tv_sec = 1, .tv_nsec = 0};

            int epfd = epoll_create1(0);
            if (epfd < 0) {
                printError("epoll_create1: ", epfd);
            }

            PGConnection pgConnection{};
            pgConnection.connect(connString);
            pgConnection.setup(epfd, &ring);


            while (isRunning) {
                io_uring_wait_cqe_timeout(&ring, cqes.data(), &ts);

                for (io_uring_cqe *&cqe: cqes) {
                    if (cqe != nullptr) {
                        if (cqe->res == -EAGAIN) {
                            io_uring_cqe_seen(&ring, cqe);
                            continue;
                        }

                        auto ptr = static_cast<PGConnection*>(io_uring_cqe_get_data(cqe));
                        if (ptr != nullptr) {
                            ptr->doNextStep(&ring, cqe->res);
                        }
                    }
                    io_uring_cqe_seen(&ring, cqe);
                }
            }

            close(epfd);
        });
    }
public:
    PGQueryProcessor(char const* connectionString)
        :connString(connectionString) {}

    ~PGQueryProcessor() {
        thrd.join();
    }

    void init() {
        processRequests();
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
