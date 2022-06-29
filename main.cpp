#include <iostream>
#include <latch>

#include "src/PGQueryProcessor.hpp"

int main() {
    std::latch latch{1};
    PGQueryProcessor p{"host=/var/run/postgresql dbname=bugseeker user=bugseeker password=28077485"};
    p.init();
    p.push(PGQBuilder("select 1 from member").build());
    latch.wait();
    return 0;
}
