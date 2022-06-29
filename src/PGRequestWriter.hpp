//
// Created by anewman on 2022-06-29.
//

#ifndef PGQUEUE_PGREQUESTWRITER_HPP
#define PGQUEUE_PGREQUESTWRITER_HPP

#include <string>
#include <liburing.h>
#include <cstring>

#include "PGQueryStructures.hpp"
#include "PGRequestReader.hpp"

class PGRequestWriter {
    static constexpr size_t BUF_SIZE = 4096;
public:
    enum PGRequestWriterState: int {
        PGRequestWriterState_NotSet,
        PGRequestWriterState_Read,
    };
private:
    std::string content;
    PGRequestWriterState state{PGRequestWriterState_NotSet};
    int writeFd;
    int readFd;
    size_t nbBytesWritten{};
    PGQueryRequest *request{};
public:
    ~PGRequestWriter() {
        delete request;
    }

    PGRequestWriter(int readFd, int writeFd, PGQueryRequest *request):readFd(readFd), writeFd(writeFd), request(request) {
        content.append(request->queryParams[0].command);
    }

    void writeDataBegin(io_uring *ring) {
        state = PGRequestWriterState_Read;
        io_uring_sqe* sqe = io_uring_get_sqe(ring);
        io_uring_prep_write(sqe, writeFd, content.c_str(), content.size(), nbBytesWritten);
        io_uring_sqe_set_data(sqe, this);
        io_uring_submit(ring);
    }

    void writeDataComplete(io_uring *ring, int res) {
        // accumulate the content
        content.erase(0, res);
        nbBytesWritten += res;

        if (content.empty()) {
            // DONE
            delete this;
        } else {
            // still more data to read
            writeDataBegin(ring);
        }
    }

    void doNextStep(io_uring *ring, int res = 0) {
        switch (state) {
            case PGRequestWriterState_NotSet:
                writeDataBegin(ring);
                break;
            case PGRequestWriterState_Read:
                writeDataComplete(ring, res);
                break;
        }
    }
};


#endif //#define PGQUEUE_PGREQUESTWRITER_HPP

