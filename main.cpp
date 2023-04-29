#include <iostream>
#include <latch>
#include <chrono>
#include <atomic>

#include "src/PGQueryProcessor.hpp"
#include "src/common/TimeUtils.hpp"

int main() {
    using namespace std::chrono_literals;
    static constexpr size_t MAX_QUERIES = 170000;
    // std::latch latch{1};
    {
        auto p = PGQueryProcessor::createInstance("host=/var/run/postgresql dbname=bugseeker user=bugseeker password=28077485", 12, 16);

        auto t = now();
        std::atomic<int> count{0};
        for (int a{}; a < 1; a += 1) {
            for (int i{}; i < MAX_QUERIES; i += 1) {
                p->push(
                        PGQBuilder("select u.user_account_id, u.email, u.hashed_password, u.salt, u.create_date, u.password_alg_id, u.session_id, u.session_end_date from user_account u where u.user_account_id=$1").addParam("df20a04e-10ae-44c1-904d-8b90bb29d486").build(),
                        [t, &count](PGResultSet&& resultSet) {
                            resultSet.rows.size();
                            if (++count == MAX_QUERIES - 1) {
                                printElapsed(t);
                            }
                        }
                    );
            }
            std::this_thread::sleep_for(1s);
        }
        delete p;
    }
    // latch.wait();
    return 0;
}
