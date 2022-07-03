#include <iostream>
#include <latch>
#include <chrono>

#include "src/PGQueryProcessor.hpp"
#include "src/common/TimeUtils.hpp"

int main() {
    using namespace std::chrono_literals;
    static constexpr size_t MAX_QUERIES = 10;
    // std::latch latch{1};
    {
        PGQueryProcessor p{"host=/var/run/postgresql dbname=bugseeker user=bugseeker password=28077485"};
        p.go();

        auto t = now();
        for (int a{}; a < 1; a += 1) {
            for (int i{}; i < MAX_QUERIES; i += 1) {
                p.push(PGQBuilder("select * from member where member_id=$1").addParam(70ul).build(), [t, i = i](PGResultSet&& resultSet) {
                    // printf("%i\n", i);
                    if (i == MAX_QUERIES - 1) {
                        printElapsed(t);
                    }
                });
            }
            std::this_thread::sleep_for(1s);
        }
    }
    // latch.wait();
    return 0;
}
