#include <iostream>
#include <latch>
#include <chrono>

#include "src/PGQueryProcessor.hpp"

int main() {
    using namespace std::chrono_literals;

    std::latch latch{1};
    PGQueryProcessor p{"host=/var/run/postgresql dbname=bugseeker user=bugseeker password=28077485"};
    p.go();
    for (int i{}; i < 10; i += 1) {
        p.push(PGQBuilder("select 1 from member").build());
        std::this_thread::sleep_for(3s);
    }
    latch.wait();
    return 0;
}
