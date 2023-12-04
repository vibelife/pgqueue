# pgqueue
High performance C++ PostgreSQL database connector (only tested in Ubuntu and Ubuntu server!)

**currently only supports running SQL queries!**

Header only lib, so you can just copy the files from "src" folder into your project, and check the CMakeLists in the root folder and make sure you have the dependencies in your project.

The test project uses the following dependency setup...
```
...

find_package(Threads)
link_libraries(${Threads})

find_package(PostgreSQL)
include_directories(/usr/include/postgresql/current)


target_link_libraries(${PROJECT_NAME} PostgreSQL::PostgreSQL ${CMAKE_THREAD_LIBS_INIT})
```

Here is some sample code...
```
// `PGQueryProcessor` is meant to be a long lived object, and when you close your application you should delete it
// connection strings can be a Unix Domain Socket for a performance boost
PGQueryProcessor *p = PGQueryProcessor::createInstance("host=/var/run/postgresql dbname=foo user=bar password=foobar");

// this is the callback after each query is executed
const auto callback = [](PGResultSet&& resultSet) {
    for (PGRow& row : resultSet.rows) {
        std::cout << row.get("car") << std::endl;
    }
};

p->push(
  PGQueryParams::Builder::create("select * from bar where car=$1")
    .addParam("jaguar")
    .build(),
  callback
);

std::this_thread::sleep_for(1s);

delete p;
```

# Performance


Test 1:

- Intel Core i9-12900KF 64GB RAM
- Ubuntu 22.04
- PostgreSQL 15
- createInstance(...) code below
```
// connection pool size = 12
// queries per connection = 16
PGQueryProcessor::createInstance("...", 12, 16);
```

Results #1: 160,000 - 165,000 queries per second (complete query round trips that return no rows, and the callback is called)

Results #1: 138,000 - 140,000 queries per second (complete query round trips that return a single row from a user table, and the callback is called)



