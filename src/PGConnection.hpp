//
// Created by anewman on 2022-06-28.
//

#ifndef PGQUEUE_PGCONNECTION_HPP
#define PGQUEUE_PGCONNECTION_HPP

#include <libpq-fe.h>
#include <atomic>
#include <string>
#include <iostream>
#include <functional>
#include <liburing.h>
#include "PGQueryStructures.hpp"

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
        PGConnectionStep_Processing,
        PGConnectionStep_Done,
    };
private:
    std::atomic<PGConnectionState> connectionState{NOT_SET};
    std::atomic<PGConnectionStep> step{PGConnectionStep_NotSet};
    std::function<void(PGResultSet&&)> callback;
    int pgfd;
    pg_conn* conn = nullptr;
private:
    static void printError(std::string const& msg) {
        std::cerr << msg << "\n";
    }

    /**
     * Populates the response with the results from the SQL query
     * @param result
     * @param response
     */
    static void handleResult(PGresult* result, PGQueryResponse* response) {
        int nbRows = PQntuples(result);
        int nbFields = PQnfields(result);

        for (int rowIndex{}; rowIndex < nbRows; rowIndex += 1) {
            PGRow row{};
            for (int fieldIndex{}; fieldIndex < nbFields; fieldIndex += 1) {
                row.addField(PQfname(result, fieldIndex), PQgetvalue(result, rowIndex, fieldIndex));
            }
            response->resultSet.rows.emplace_back(std::move(row));
        }
    }
public:
    ~PGConnection() {
        if (PQstatus(conn) == CONNECTION_OK) {
            PQfinish(conn);
        }
        close(pgfd);
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

    [[nodiscard]] bool isReady() const {
        return connectionState == READY;
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
                        printError("Could not connectAll to the database. Check the connection string.");
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
        io_uring_sqe_set_data(sqe, (void*) this);
        io_uring_submit(ring);
    }

    /**
     * Sends the query to the database
     * @param request
     */
    bool sendRequestIfReady(PGQueryRequest* request) {
        if (connectionState == READY) {
            connectionState = NOT_READY;
            std::swap(callback, request->callback);

            int res{};
            switch (request->queryParams->type) {
                case PGQueryParams::PLAIN_QUERY:
                    res = PQsendQuery(conn, request->queryParams->command);
                    break;
                case PGQueryParams::QUERY_WITH_PARAMS:
                    res = PQsendQueryParams(
                            conn,
                            request->queryParams->command,
                            request->queryParams->nParams,
                            request->queryParams->paramTypes,
                            request->queryParams->paramValues,
                            request->queryParams->paramLengths,
                            request->queryParams->paramFormats,
                            request->queryParams->resultFormat
                    );
                    break;
            }

            if (res == 0) {
                printError("PQsendQuery");
                exit(EXIT_FAILURE);
            }
            return true;
        }

        return false;
    }

    void handleQueryResponseBegin() {
        step = PGConnectionStep_Processing;
        if (PQflush(conn) == 0) {
            if (PQconsumeInput(conn) == 0) {
                printError("PQconsumeInput");
                exit(EXIT_FAILURE);
            }

            if (PQisBusy(conn) == 0) {
                auto response = new PGQueryResponse();
                std::swap(response->callback, callback);

                PGresult* result = PQgetResult(conn);
                while (result != nullptr) {
                    switch (PQresultStatus(result)) {
                        case PGRES_TUPLES_OK:
                            handleResult(result, response);
                            break;
                        case PGRES_EMPTY_QUERY:
                        case PGRES_COMMAND_OK:
                            // no data from the server
                            break;
                        case PGRES_COPY_OUT:
                            break;
                        case PGRES_COPY_IN:
                            break;
                        case PGRES_BAD_RESPONSE:
                            break;
                        case PGRES_NONFATAL_ERROR:
                            break;
                        case PGRES_FATAL_ERROR:
                            response->resultSet.errorMsg = PQresultErrorMessage(result);
                            if (response->resultSet.errorMsg.empty()) {
                                response->resultSet.errorMsg = PQerrorMessage(conn);
                            }
                            break;
                        case PGRES_COPY_BOTH:
                            break;
                        case PGRES_SINGLE_TUPLE:
                            break;
                        case PGRES_PIPELINE_SYNC:
                            break;
                        case PGRES_PIPELINE_ABORTED:
                            break;
                    }
                    result = PQgetResult(conn);
                }

                PQclear(result);

                // processor->pushResponse(response);
                connectionState = READY;
                step = PGConnectionStep_NotSet;
            }
        }
    }

    void doNextStep(io_uring *ring, int res) {
        switch (step) {
            case PGConnectionStep_NotSet:
                handleQueryResponseBegin();
                break;
        }
    }
};

#endif //PGQUEUE_PGCONNECTION_HPP
