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

## Performance Test 1:

- Intel Core i9-12900KF 64GB RAM
- Ubuntu 22.04
- PostgreSQL 15
- createInstance(...) code below
```
// connection pool size = 12
// queries per connection = 16
PGQueryProcessor::createInstance("...", 12, 16);
```

Results #1: **160,000 - 165,000** queries per second (complete query round trips that return no rows, and the callback is called)

Results #2: **138,000 - 140,000** queries per second (complete query round trips that return a single row from a user table, and the callback is called)

More details: TODO


## Performance Test 2:

- Intel Xeon E-2136 @ 3.30GHz 32GB RAM
- Ubuntu 22.04
- PostgreSQL 15
- createInstance(...) code below
```
// connection pool size = 6
// queries per connection = 8
PGQueryProcessor::createInstance("...", 6, 8);
```
Results #1: TODO

Results #2: TODO

More details: TODO


# Copyright and license

This project uses the MIT license

```
Copyright (c) 2023 Vibelife

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
