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
#include <queue>
#include "PGQueryStructures.hpp"

class PGConnection {
public:

    enum PGConnectionResult {
        PGConnectionResult_NotSet,
        PGConnectionResult_Ok,
        PGConnectionResult_Failed
    };

    enum PGConnectionState {
        PGConnectionState_NotSet,
        PGConnectionState_Connected,
    };

private:
    std::atomic<PGConnectionState> connectionState{PGConnectionState_NotSet};
    std::queue<std::function<void(PGResultSet&&)>> callbacks{};
    int pgfd{};
    pg_conn* conn = nullptr;
    unsigned nbMaxPending{4};
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
    explicit PGConnection(unsigned nbMaxPending = 4)
        :nbMaxPending(nbMaxPending)
    {}

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

    [[nodiscard]] bool isReady() const {
        return callbacks.size() < nbMaxPending;
    }

    [[nodiscard]] bool isDone() const {
        return callbacks.empty();
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
            return PGConnectionResult::PGConnectionResult_Failed;
        }

        // set the connection to nonblocking
        if (PQsetnonblocking(conn, 1) == -1) {
            std::string err{PQerrorMessage(conn)};
            printError("Could not set the connection to nonblocking - " + err);
            return PGConnectionResult::PGConnectionResult_Failed;
        }

        // ensure we can continue
        if (PQstatus(conn) == CONNECTION_BAD) {
            std::string err{PQerrorMessage(conn)};
            printError("The connection is bad - " + err);
            return PGConnectionResult::PGConnectionResult_Failed;
        }

        PGConnectionResult retVal{PGConnectionResult_NotSet};
        while (retVal == PGConnectionResult_NotSet) {
            // tries to connect to the database
            if (connectionState != PGConnectionState::PGConnectionState_Connected) {
                switch (PQconnectPoll(conn)) {
                    case PGRES_POLLING_OK:
                        pgfd = PQsocket(conn);
                        if (!PQenterPipelineMode(conn)) {
                            std::cerr << "Could not enter pipeline mode: PQenterPipelineMode(...)\n";
                            exit(EXIT_FAILURE);
                        }
                        retVal = PGConnectionResult::PGConnectionResult_Ok;
                        break;
                    case PGRES_POLLING_FAILED:
                        printError("Could not connectAllURing to the database. Check the connection string.");
                        retVal = PGConnectionResult::PGConnectionResult_Failed;
                        break;
                    default:
                        break;
                }
            }
        }
        return retVal;
    }
    /**
     * Sets up io_uring
     * @param epfd
     * @param ring
     */
    void setupURing(int epfd, io_uring *ring) const {
        io_uring_sqe* sqe = io_uring_get_sqe(ring);
        struct epoll_event ev{};
        ev.events = EPOLLIN | EPOLLOUT | EPOLLHUP | EPOLLERR | EPOLLRDHUP;
        ev.data.fd = pgfd;
        io_uring_prep_epoll_ctl(sqe, epfd, pgfd, EPOLL_CTL_ADD, &ev);
        io_uring_sqe_set_data(sqe, (void*) this);
        io_uring_submit(ring);
    }

    /**
     * Sets up epoll
     * @param epfd
     */
    void setupEPoll(int epfd) const {
        struct epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = pgfd;

        if (epoll_ctl(epfd, EPOLL_CTL_ADD, pgfd, &ev) == -1) {
            printError("epoll_ctl");
            exit(EXIT_FAILURE);
        }
    }

    /**
     * Sends the query to the database
     * @param request
     */
    bool sendRequestIfReady(PGQueryRequest* request) {
        if (isReady()) {
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

            callbacks.push(request->callback);

            res = PQflush(conn);
            res = PQpipelineSync(conn);
            return true;
        }

        return false;
    }

    void handleQueryResponse(rigtorp::MPMCQueue<PGQueryResponse*> &responses) {
        checkPGReady:
        if (PQconsumeInput(conn) == 0) {
            printError("PQconsumeInput");
            exit(EXIT_FAILURE);
        }

        int pgIsBusy = PQisBusy(conn);
        if (pgIsBusy == 1) {
            goto checkPGReady;
        }
        if (pgIsBusy == 0) {
            PGresult* result{};

            // these loops ensure that every callback is fired
            size_t nb{callbacks.size()};
            for (size_t i{0}; i < nb; i += 1) {
                // the logic for pipeline handling is outlined here:
                // https://www.postgresql.org/docs/14/libpq-pipeline-mode.html
                while ((result = PQgetResult(conn)) != nullptr) {
                    int status = PQresultStatus(result);
                    if (status == PGRES_PIPELINE_SYNC) {
                        PQclear(result);
                        continue;
                    }

                    auto response = new PGQueryResponse();
                    std::swap(response->callback, callbacks.front());
                    callbacks.pop();

                    switch (status) {
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
                        default:
                            break;
                    }

                    responses.push(response);

                    PQclear(result);
                }
            }
        }
    }

    void doNextStep(int res, rigtorp::MPMCQueue<PGQueryResponse*> &responses) {
        handleQueryResponse(responses);
    }
};

#endif //PGQUEUE_PGCONNECTION_HPP
