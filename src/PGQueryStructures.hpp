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
};

static constexpr auto NOOP = [](auto){};

struct PGQueryResponse {
    static PGQueryResponse* poison() {
        return new PGQueryResponse{true};
    }

    bool isPoison{};
    PGResultSet resultSet{};
    std::function<void(PGResultSet&&)> callback = NOOP;
};

struct PGQueryRequest {
    ~PGQueryRequest() {
        delete queryParams;
    }

    static PGQueryRequest* poison() {
        return new PGQueryRequest{true};
    }

    bool isPoison{};
    PGQueryParams* queryParams{nullptr};
    std::function<void(PGResultSet&&)> callback{nullptr};
};



#endif //PGQUEUE_PGQUERYSTRUCTURES_HPP
