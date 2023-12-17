#include <iostream>
#include <latch>
#include <chrono>
#include <atomic>

#include "src/PGQueryProcessor.hpp"
#include "src/common/TimeUtils.hpp"

int main() {
    using namespace std::chrono_literals;
    static constexpr size_t NB_QUERIES_TO_RUN = 230000; /* increase this number until the time is 1.0 seconds */

    {
        // Create an instance of [PGQueryProcessor] that is connected to the database
        // - destructing the instance will disconnect from the database
        PGQueryProcessor *p = PGQueryProcessor::createInstance("host=/var/run/postgresql dbname=bugseeker user=bugseeker password=28077485", 19);

        // used for timing
        const auto t = now();
        // used for timing
        std::atomic<int> count{0};
        // this is the callback after each query is executed - also used for timing
        const auto cb = [&t, &count](PGResultSet&& resultSet) {
            if (++count == NB_QUERIES_TO_RUN) {
                printElapsed(t);
            }
        };


        // send each query to the database in a tight loop
        for (int i{}; i < NB_QUERIES_TO_RUN; i += 1) {
            p->push(
                PGQueryParams::Builder::create("select u.user_account_id, u.email, u.hashed_password, u.salt, u.create_date, u.password_alg_id, u.session_id, u.session_end_date from user_account u where u.user_account_id=$1")
                    .addParam("df20a04e-10ae-44c1-904d-8b90bb29d486")
                    .build(),
                cb
            );
        }

        std::this_thread::sleep_for(1s);

        // when you destruct a [PGQueryProcessor] it will wait until the currently running queries are done first
        delete p;
    }

    return 0;
}
