#include <iostream>
#include <latch>
#include <chrono>

#include "src/PGQueryProcessor.hpp"
#include "src/common/TimeUtils.hpp"

int main() {
    using namespace std::chrono_literals;
    static constexpr size_t MAX_QUERIES = 20000;
    // std::latch latch{1};
    {
        auto p = PGQueryProcessor::createInstance("host=/var/run/postgresql dbname=bugseeker user=bugseeker password=28077485");

        auto t = now();
        for (int a{}; a < 1; a += 1) {
            for (int i{}; i < MAX_QUERIES; i += 1) {
                p->push(PGQBuilder("select u.user_account_id, u.email, u.hashed_password, u.salt, u.create_date, u.password_alg_id, u.session_id, u.session_end_date from user_account u where u.user_account_id=$1").addParam("45381982-f202-47f4-97d3-cb212afbffa8").build(), [t, i = i](PGResultSet&& resultSet) {
                    // printf("%i\n", i);
                    if (i == MAX_QUERIES - 1) {
                        printElapsed(t);
                    }
                });
            }
            std::this_thread::sleep_for(1s);
        }
        delete p;
    }
    // latch.wait();
    return 0;
}
