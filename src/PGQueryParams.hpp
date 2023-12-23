#ifndef PGQUEUE_PGQUERYPARAMS_HPP
#define PGQUEUE_PGQUERYPARAMS_HPP

#include <vector>
#include <cstring>
#include <string>
#include <postgres.h>
#include <libpq-fe.h>
#include <catalog/pg_type.h>
#include <parser/parse_type.h>
#include "libs/rapidjson/writer.h"

template <typename T>
concept toJson = requires (T const v) {
    {v.toSqlParam()} -> std::convertible_to<std::string>;
};

template <typename T>
concept writeJson = requires (T const v, rapidjson::Writer<rapidjson::StringBuffer>& writer) {
    {v.writeJsonParam(writer)} -> std::convertible_to<void>;
};

/**
 * To find the postgresql OID of any value execute the following query in posteges "SELECT pg_typeof(???)::oid"
 * replaceDashWithUnderscore ??? with the value you want the OID of, then look up the result in <catalog/pg_type.h>
 * For example "SELECT pg_typeof(1)::oid" returns "23". When you search "23" in <catalog/pg_type.h> you will see
 * that is maps to "INT4OID".
 */
struct PGParam {
    Oid oid;
    std::string value;

    explicit PGParam(Oid oid, std::string&& value): oid(oid), value(std::move(value)) {}
    explicit PGParam(Oid oid): oid(oid) {}
};

struct PGJsonArray: public PGParam {
    template <typename T>
    explicit PGJsonArray(std::vector<T>&& value) requires toJson<T>:PGParam(JSONOID) {
        this->value.append("[");
        for (T const& v: value) {
            this->value.append(v.toSqlParam());
            this->value.append(",");
        }

        // remove the trailing comma
        if (!value.empty()) {
            this->value.erase(this->value.size() - 1);
        }

        this->value.append("]");
    }

    template <typename T>
    explicit PGJsonArray(std::vector<T>&& value) requires writeJson<T>:PGParam(JSONOID) {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

        writer.StartArray();

        for (T const& v: value) {
            v.writeJsonParam(writer);
        }

        writer.EndArray();

        this->value.append(buffer.GetString());
    }

    explicit PGJsonArray(std::string&& value): PGParam(JSONOID, std::move(value)) {}
};

struct PGVarchar: public PGParam {
    explicit PGVarchar(std::string&& value): PGParam(VARCHAROID, std::move(value)){}
    explicit PGVarchar(char const* value): PGParam(VARCHAROID, value){}
};

struct PGFloat: public PGParam {
    explicit PGFloat(double value): PGParam(FLOAT8OID, std::move(std::to_string(value))) {}
};

struct PGBigUInt: public PGParam {
    explicit PGBigUInt(unsigned long value): PGParam(INT8OID, std::move(std::to_string(value))) {}
};

struct PGBigInt: public PGParam {
    explicit PGBigInt(long value): PGParam(INT8OID, std::move(std::to_string(value))) {}
};

struct PGBool: public PGParam {
    explicit PGBool(bool value): PGParam(BOOLOID, std::move(std::to_string(value))) {}
};

struct PGInt: public PGParam {
    explicit PGInt(int value): PGParam(INT4OID, std::move(std::to_string(value))) {}
};

struct PGUInt: public PGParam {
    explicit PGUInt(unsigned int value): PGParam(INT4OID, std::move(std::to_string(value))) {}
};

class PGQueryParams {
public:
#define PGQBuilder(x) PGQueryParams::Builder::create(x)

    enum QueryType: int {
        PLAIN_QUERY,
        QUERY_WITH_PARAMS
    };
public:
    QueryType type = PLAIN_QUERY;

    std::string command{};
    /**
     * The number of parameters supplied; it is the length of the arrays
     * paramTypes[], paramValues[], paramLengths[], and paramFormats[].
     * (The array pointers can be NULL when nParams is zero.)
     */
    int nParams{};
    /**
     * Specifies, by OID, the data types to be assigned to the parameter symbols. If
     * paramTypes is NULL, or any particular element in the array is zero, the server
     * infers a data type for the parameter symbol in the same way it would do for an
     * untyped literal string.
     */
    Oid* paramTypes = nullptr;
    /**
     * Specifies the actual values of the parameters. A null pointer in this array means the
     * corresponding parameter is null; otherwise the pointer points to a zero-terminated
     * text string (for text format) or binary data in the format expected by the server
     * (for binary format).
     */
    char** paramValues = nullptr;
    /**
     * Specifies the actual data lengths of binary-format parameters. It is ignored for null
     * parameters and text-format parameters. The array pointer can be null when there are no binary parameters.
     */
    int* paramLengths = nullptr;
    /**
     * Specifies whether parameters are text (put a zero in the array entry for the corresponding
     * parameter) or binary (put a one in the array entry for the corresponding parameter).
     * If the array pointer is null then all parameters are presumed to be text strings.
     */
    int* paramFormats = nullptr;
    /**
     * Specify zero to obtain results in text format, or one to obtain results in binary format.
     * (There is not currently a provision to obtain different result columns in different
     * formats, although that is possible in the underlying protocol.)
     */
    int resultFormat{};
public:
    ~PGQueryParams() {
        if (paramTypes != nullptr) {
            free(paramTypes);
        }

        if (paramLengths != nullptr) {
            free(paramLengths);
        }

        if (paramFormats != nullptr) {
            free(paramFormats);
        }

        if (paramValues != nullptr) {
            // free each value
            for (int i = 0; i < nParams; i += 1) {
                free(paramValues[i]);
            }
            // free the array itself
            free(paramValues);
        }
    }

    template<class PGQueryParams_T = PGQueryParams>
    class Builder {
    private:
        PGQueryParams_T managed{};
        std::vector<PGParam> params{};
    public:
        static Builder<PGQueryParams> create(std::string&& sql) {
            auto retVal = Builder<PGQueryParams>{};
            retVal.managed.command = std::move(sql);
            return retVal;
        }

        /**
         * Returns an empty builder instance
         * @return
         */
        static Builder create() {
            return Builder{};
        }

        /**
         * Builds out all of the fields that need to be passed to [PQsendQueryParams]
         * @return
         */
        PGQueryParams&& build() {
            static constexpr auto OID_SIZE = sizeof(Oid);
            static constexpr auto CHAR_PTR_SIZE = sizeof(char*);

            managed.nParams = static_cast<int>(params.size());
            managed.paramTypes = static_cast<Oid*>(calloc((int)params.size(), OID_SIZE));


            managed.paramValues = static_cast<char**>(malloc(params.size() * CHAR_PTR_SIZE));
            size_t paramTypeIndex{};
            for (PGParam const& param: params) {
                managed.paramTypes[paramTypeIndex] = param.oid;
                managed.paramValues[paramTypeIndex] = static_cast<char*>(calloc(param.value.size() + 1, CHAR_PTR_SIZE));
                memmove(managed.paramValues[paramTypeIndex++], param.value.c_str(), param.value.size());
            }

            // at this point the fields are ready to be passed to postgresql
            return std::move(managed);
        }

        /**
         * Returns the number of parameters added so far
         * @return
         */
        [[nodiscard]]
        size_t getNbParams() const {
            return this->params.size();
        }

        /**
         * Sets the SQL
         * @param sql
         * @return
         */
        Builder& setSql(std::string &&sql) {
            managed.command = std::move(sql);
            return *this;
        }

        /**
         * Adds a json[] param. The type param needs to have a toJson() method that returns the JSON.
         * @param value
         * @return
         */
        template <typename T>
        Builder& addJsonArrayParam(std::vector<T>&& value) requires toJson<T> {
            addParam(PGJsonArray{std::move(value)});
            return *this;
        }

        /**
         * Adds a json[] param. The type param needs to have a writeJson(...) method that write the JSON object into the writer.
         * @param value
         * @return
         */
        template <typename T>
        Builder& addJsonArrayParam(std::vector<T>&& value) requires writeJson<T> {
            addParam(PGJsonArray{std::move(value)});
            return *this;
        }

        /**
         * Adds a json[] param
         * @param value
         * @return
         */
        Builder& addJsonArrayParam(std::string&& value) {
            addParam(PGJsonArray{std::move(value)});
            return *this;
        }

        /**
         * Adds a varchar param
         * @param value
         * @return
         */
        Builder& addParam(std::string&& value) {
            addParam(PGVarchar{std::move(value)});
            return *this;
        }

        /**
         * Adds a varchar param
         * @param value
         * @return
         */
        Builder& addParam(std::string_view value) {
            addParam(PGVarchar{std::string{value}});
            return *this;
        }

        /**
         * Adds a varchar param
         * @param value
         * @return
         */
        Builder& addParam(char const* value) {
            addParam(PGVarchar{value});
            return *this;
        }

        /**
         * Adds a varchar param. This overload copies the string.
         * @param value
         * @return
         */
        Builder& addParam(std::string const& value) {
            addParam(PGVarchar{std::string(value)});
            return *this;
        }

        /**
         * Adds an unsigned long param
         * @param value
         * @return
         */
        Builder& addParam(unsigned long value) {
            addParam(PGBigUInt{value});
            return *this;
        }

        /**
         * Adds an int param
         * @param value
         * @return
         */
        Builder& addParam(int value) {
            addParam(PGInt{value});
            return *this;
        }

        /**
         * Adds a double param
         * @param value
         * @return
         */
        Builder& addParam(double value) {
            addParam(PGFloat{value});
            return *this;
        }

        /**
         * Adds a bool param
         * @param value
         * @return
         */
        Builder& addParam(bool value) {
            addParam(PGBool{value});
            return *this;
        }

        /**
         * Adds an unsigned int param
         * @param value
         * @return
         */
        Builder& addParam(unsigned int value) {
            addParam(PGUInt{value});
            return *this;
        }
    private:
        void addParam(PGParam &&param) {
            managed.type = QUERY_WITH_PARAMS;
            params.emplace_back(std::move(param));
        }
    };

    static PGQueryParams::Builder<> createBuilder(std::string &&sql) {
        return PGQueryParams::Builder<PGQueryParams>::create(std::move(sql));
    };
};

static PGQueryParams::Builder<> q(std::string&& sql) {
    return PGQueryParams::Builder<PGQueryParams>::create(std::move(sql));
};

#endif //PGQUEUE_PGQUERYPARAMS_HPP
