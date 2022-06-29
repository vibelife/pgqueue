//
// Created by anewman on 2022-06-28.
//

#ifndef PGQUEUE_PGEVENTHANDLER_HPP
#define PGQUEUE_PGEVENTHANDLER_HPP

#include <liburing.h>

class PGEventHandler {
public:
    void doNextStep(struct io_uring *ring, const int res) {};
};

#endif //PGQUEUE_PGEVENTHANDLER_HPP
