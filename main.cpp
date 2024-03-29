#include <iostream>
#include <chrono>
#include <atomic>

#include "src/PGQueryProcessor.hpp"
#include "src/common/TimeUtils.hpp"

int main() {
    using namespace std::chrono_literals;
    static constexpr size_t NB_QUERIES_TO_RUN = 50000; /* increase this number until the time is 1.0 seconds */

    {
        // Create an instance of [PGQueryProcessor] that is connected to the database
        // - destructing the instance will disconnect from the database
        //PGQueryProcessor *p = PGQueryProcessor::createInstance("host=/var/run/postgresql dbname=bugseeker user=bugseeker password=28077485", 4, 50, 178000, 2);
        PGQueryProcessor *p = PGQueryProcessor::createInstance("postgres://bugseeker:28077485@localhost:5432/bugseeker", 16, 16, 178000, 2);

        // wait for connection pool to connect, then we start timing.
        std::this_thread::sleep_for(500ms);

        // used for timing
        const auto t = now();
        // used for timing
        std::atomic<int> count{0};
        // this is the callback after each query is executed - also used for timing
        const auto cb = [&t, &count](PGResultSet&& resultSet) {
            if (++count == NB_QUERIES_TO_RUN) {
                printElapsed(t, "after callback");
            }
        };

        //std::string p1{"6d7382fe-6058-4c83-aaaa-ea6e24293479"};
        //std::string p2{"https://front-test.com/account-address.html"};
        std::string p1{"bool"};

        // send each query to the database in a tight loop
        for (int i{}; i < NB_QUERIES_TO_RUN; i += 1) {
            p->push(
                /*PGQueryParams::createBuilder("select u.user_account_id, u.email, u.hashed_password, u.salt, u.create_date, u.password_alg_id, u.session_id, u.session_end_date from user_account u where u.user_account_id=$1")*/
                PGQueryParams::createBuilder("select * from pg_catalog.pg_type where typname = $1")
                    .addParam(p1)
                    //.addParam(p2)
                    .build(),
                    cb
            );
        }

        // when you destruct a [PGQueryProcessor] it will wait until the currently running queries are done first
        delete p;
    }

    return 0;
}
