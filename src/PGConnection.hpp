//
// Created by anewman on 2022-06-28.
//

#ifndef PGQUEUE_PGCONNECTION_HPP
#define PGQUEUE_PGCONNECTION_HPP

#include <libpq-fe.h>
#include <string>
#include <iostream>

class PGConnection {
public:
    enum PGConnectionResult {
        OK,
        FAILED
    };

    enum PGConnectionState {
        NOT_SET,
        CONNECTING,
        READY,
        NOT_READY
    };


    enum PGConnectionStep {
        PGConnectionStep_NotSet,
    };
private:
    PGConnectionState connectionState{NOT_SET};
    PGConnectionStep step{PGConnectionStep_NotSet};
    int pgfd;
    pg_conn* conn = nullptr;
private:
    static void printError(std::string const& msg) {
        std::cerr << msg << "\n";
    }

public:
    ~PGConnection() {
        if (PQstatus(conn) == CONNECTION_OK) {
            PQfinish(conn);
        }
    }

    /**
     * Returns the file descriptor for the database connection
     * @return
     */
    [[nodiscard]] int fd() const {
        return pgfd;
    }

    [[nodiscard]] PGConnectionState getState() const {
        return connectionState;
    }

    /**
     * Blocking database connection
     * @param connectionString
     * @return
     */
    PGConnectionResult connect(char const* connectionString) {
        conn = PQconnectStart(connectionString);

        // ensure the allocation was ok
        if (conn == nullptr) {
            printError("Could not instantiate the postgres connection object with the provided connection string");
            return PGConnectionResult::FAILED;
        }

        // set the connection to nonblocking
        if (PQsetnonblocking(conn, 1) == -1) {
            std::string err{PQerrorMessage(conn)};
            printError("Could not set the connection to nonblocking - " + err);
            return PGConnectionResult::FAILED;
        }

        // ensure we can continue
        if (PQstatus(conn) == CONNECTION_BAD) {
            std::string err{PQerrorMessage(conn)};
            printError("The connection is bad - " + err);
            return PGConnectionResult::FAILED;
        }

        bool isPolling{};
        do {
            isPolling = false;
            // tries to connect to the database
            if (connectionState != PGConnectionState::READY) {
                switch (PQconnectPoll(conn)) {
                    case PGRES_POLLING_OK:
                        connectionState = PGConnectionState::READY;
                        pgfd = PQsocket(conn);
                        return PGConnectionResult::OK;
                    case PGRES_POLLING_FAILED:
                        printError("Could not connect to the database. Check the connection string.");
                        return PGConnectionResult::FAILED;
                    default:
                        isPolling = true;
                        break;
                }
            }
        } while (isPolling);
        return PGConnectionResult::FAILED;
    }

    void setup(int epfd, io_uring *ring) const {
        io_uring_sqe* sqe = io_uring_get_sqe(ring);
        struct epoll_event ev{};
        ev.events = EPOLLIN | EPOLLOUT | EPOLLHUP | EPOLLERR | EPOLLRDHUP;
        ev.data.fd = pgfd;
        io_uring_prep_epoll_ctl(sqe, epfd, pgfd, EPOLL_CTL_ADD, &ev);
        io_uring_submit(ring);
    }

    void doNextStep(io_uring *ring, int res) {
        switch (step) {
            case PGConnectionStep_NotSet:
                break;
        }
    }
};

#endif //PGQUEUE_PGCONNECTION_HPP
