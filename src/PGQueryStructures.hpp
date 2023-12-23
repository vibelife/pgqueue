#ifndef PGQUEUE_PGQUERYSTRUCTURES_HPP
#define PGQUEUE_PGQUERYSTRUCTURES_HPP

#include <functional>
#include <cstdio>
#include <unordered_map>
#include <vector>

#include "PGQueryParams.hpp"

#undef printf

class PGRow {
private:
    bool isCleared{};
    std::unordered_map<std::string, std::string> data{};
public:
    PGRow() = default;

    PGRow(PGRow &&other)  noexcept {
        std::swap(this->isCleared, other.isCleared);
        std::swap(this->data, other.data);
    }

    PGRow(PGRow const&other) noexcept = default;

    PGRow& operator= (PGRow const&other) noexcept = default;

    /**
     * Returns true if the value is numeric
     * @param v
     * @return
     */
    static bool isNumeric(std::string const& v) {
        return !v.empty() && std::all_of(v.cbegin(),  v.cend(), isdigit);
    }

    void addField(std::string&& key, std::string&& value) {
        data.emplace(std::move(key), std::move(value));
    }

    /**
     * Returns an unsigned long to the caller, or the default
     * @param columnName
     * @param defaultValue
     * @return
     */
    unsigned long get(std::string&& columnName, unsigned long defaultValue) {
        auto it = data.find(columnName);
        return it == data.end()
               ? defaultValue
               : isNumeric(it->second)
                 ? std::stoul(it->second)
                 : defaultValue;
    }

    /**
     * Returns a std::string to the caller, or the default
     * @param columnName
     * @param defaultValue
     * @return
     */
    std::string get(std::string&& columnName, std::string&& defaultValue) {
        auto it = data.find(columnName);
        return it == data.end() ? std::move(defaultValue) : std::move(it->second);
    }

    /**
     * Returns a std::string to the caller, or the default
     * @param columnName
     * @param defaultValue
     * @return
     */
    std::string get(std::string&& columnName) {
        return get(std::move(columnName), "");
    }
};

class PGResultSet {
public:
    std::string errorMsg{};
    std::vector<PGRow> rows{};

    PGResultSet() = default;

    PGResultSet(PGResultSet &&other) noexcept {
        std::swap(errorMsg, other.errorMsg);
        std::swap(rows, other.rows);
    }

    PGResultSet& operator=(PGResultSet &&other) noexcept {
        std::swap(errorMsg, other.errorMsg);
        std::swap(rows, other.rows);
        return *this;
    }

    PGResultSet(PGResultSet const&other) noexcept {
        errorMsg = other.errorMsg;
        std::copy(other.rows.begin(), other.rows.end(), rows.begin());
    }};

static constexpr auto NOOP = [](auto){};

struct PGQueryResponse {
    PGQueryResponse() = default;
    PGQueryResponse(PGQueryResponse &&other)  noexcept {
        std::swap(this->resultSet, other.resultSet);
        std::swap(this->callback, other.callback);
    }
    PGQueryResponse& operator=(PGQueryResponse &&other)  noexcept {
        std::swap(this->resultSet, other.resultSet);
        std::swap(this->callback, other.callback);
        return *this;
    }

    PGResultSet resultSet{};
    std::function<void(PGResultSet&&)> callback = NOOP;
};

struct PGQueryRequest {
    PGQueryRequest() = default;
    PGQueryRequest(PGQueryParams &&queryParams, std::function<void(PGResultSet&&)> &&callback)
        : callback(std::move(callback)) {
        std::swap(this->queryParams, queryParams);
    }

    PGQueryRequest(PGQueryRequest &&other)  noexcept {
        std::swap(this->queryParams, other.queryParams);
        std::swap(this->callback, other.callback);
    }

    PGQueryRequest& operator=(PGQueryRequest &&other)  noexcept {
        std::swap(this->queryParams, other.queryParams);
        std::swap(this->callback, other.callback);
        return *this;
    }

    PGQueryParams queryParams{};
    std::function<void(PGResultSet&&)> callback{nullptr};
};



#endif //PGQUEUE_PGQUERYSTRUCTURES_HPP
