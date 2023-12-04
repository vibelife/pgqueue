//
// Created by anewman on 2022-07-01.
//

#ifndef PGQUEUE_TIMEUTILS_HPP
#define PGQUEUE_TIMEUTILS_HPP

#include <iostream>
#include <ctime>

#undef printf

/**
 * Returns the current time
 * @param startTime
 */
static timespec now() {
    struct timespec retVal{};
    clock_gettime(CLOCK_REALTIME, &retVal);
    return retVal;
}

/**
 * sets the timer values
 * @param t1
 * @param t2
 * @param td
 */
static timespec getTimeSpec(struct timespec const& t1, struct timespec const& t2) {
    // clock_gettime(CLOCK_REALTIME, &t2);
    static constexpr auto NS_PER_SECOND = 1000000000;

    struct timespec retVal{};
    retVal.tv_nsec = t2.tv_nsec - t1.tv_nsec;
    retVal.tv_sec  = t2.tv_sec - t1.tv_sec;
    if (retVal.tv_sec > 0 && retVal.tv_nsec < 0) {
        retVal.tv_nsec += NS_PER_SECOND;
        retVal.tv_sec--;
    } else if (retVal.tv_sec < 0 && retVal.tv_nsec > 0) {
        retVal.tv_nsec -= NS_PER_SECOND;
        retVal.tv_sec++;
    }
    return retVal;
}

/**
 * Prints the elapsed time between [startTime] and now
 * @param startTime
 */
static void printElapsed(struct timespec const& startTime, const char* text = "") {
    struct timespec t2{};
    clock_gettime(CLOCK_REALTIME, &t2);
    struct timespec td = getTimeSpec(startTime, t2);
    printf("%s - %d.%.9ld seconds\n", text, (int)td.tv_sec, td.tv_nsec);
}

#endif //PGQUEUE_TIMEUTILS_HPP
