//
// Created by anewman on 2022-06-29.
//

#ifndef PGQUEUE_PGREQUESTREADER_HPP
#define PGQUEUE_PGREQUESTREADER_HPP

#include <string>
#include <liburing.h>
#include <cstring>

class PGRequestReader {
    static constexpr size_t BUF_SIZE = 4096;
public:
    enum PGRequestReaderState: int {
        PGRequestReaderState_NotSet,
        PGRequestReaderState_Read,
    };
private:
    std::string content;
    PGRequestReaderState state{PGRequestReaderState_NotSet};
    char rbuf[BUF_SIZE]{};
    int fd;
public:
    PGRequestReader(int fd) :fd(fd) {}

    void readDataBegin(io_uring *ring) {
        state = PGRequestReaderState_Read;
        io_uring_sqe* sqe = io_uring_get_sqe(ring);
        io_uring_prep_read(sqe, fd, rbuf, BUF_SIZE, content.size());
        io_uring_sqe_set_data(sqe, this);
        io_uring_submit(ring);
    }

    void readDataComplete(io_uring *ring, int res) {
        // accumulate the content
        content.append(rbuf, res);
        // clear out the buffer
        memset(rbuf, 0, BUF_SIZE);

        if (res == 0) {
            // DONE
            delete this;
        } else {
            // still more data to read
            readDataBegin(ring);
        }
    }

    void doNextStep(io_uring *ring, int res) {
        switch (state) {
            case PGRequestReaderState_NotSet:
                readDataBegin(ring);
                break;
            case PGRequestReaderState_Read:
                readDataComplete(ring, res);
                break;
        }
    }
};

#endif //PGQUEUE_PGREQUESTREADER_HPP
