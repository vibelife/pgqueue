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

    explicit PGJsonArray(std::string&& value): PGParam(JSONOID) {
        this->value.swap(value);
    }
};

struct PGVarchar: public PGParam {
    explicit PGVarchar(std::string&& value): PGParam(VARCHAROID, std::move(value)){}
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

    char* command = nullptr;
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
    const int* paramLengths = nullptr;
    /**
     * Specifies whether parameters are text (put a zero in the array entry for the corresponding
     * parameter) or binary (put a one in the array entry for the corresponding parameter).
     * If the array pointer is null then all parameters are presumed to be text strings.
     */
    const int* paramFormats = nullptr;
    /**
     * Specify zero to obtain results in text format, or one to obtain results in binary format.
     * (There is not currently a provision to obtain different result columns in different
     * formats, although that is possible in the underlying protocol.)
     */
    int resultFormat{};
public:
    ~PGQueryParams() {
        free(command);
        free((void*) paramTypes);
        free((void*) paramLengths);
        free((void*) paramFormats);

        // free each value
        for (int i = 0; i < nParams; i += 1) {
            free((void*) paramValues[i]);
        }
        // free the array itself
        free((void*) paramValues);
    }

    class Builder {
    private:
        PGQueryParams* managed = new PGQueryParams{};
        std::vector<PGParam*> params{};
    public:
        static Builder create(std::string&& sql) {
            auto retVal = Builder{};
            retVal.managed->command = static_cast<char*>(calloc(sql.size() + 1, sizeof(char)));
            memmove((void*)retVal.managed->command, sql.c_str(), sql.size());
            return retVal;
        }

        static Builder create(char const* sql) {
            return create(sql, strlen(sql));
        }

        static Builder create(char const* sql, size_t sqlLength) {
            Builder retVal{};
            retVal.managed->command = static_cast<char*>(calloc(sqlLength + 1, sizeof(char)));
            memmove((void*)retVal.managed->command, sql, sqlLength);
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
        PGQueryParams* build() {
            static constexpr auto OID_SIZE = sizeof(Oid);
            static constexpr auto CHAR_PTR_SIZE = sizeof(char*);

            managed->nParams = (int)params.size();
            managed->paramTypes = static_cast<Oid*>(calloc((int)params.size(), OID_SIZE));

            size_t nbBytes{};

            size_t paramTypeIndex{};
            for (PGParam* param: params) {
                nbBytes += (sizeof(char) * param->value.size()) + 1;
                managed->paramTypes[paramTypeIndex++] = param->oid;
            }

            managed->paramValues = static_cast<char**>(calloc(managed->nParams, CHAR_PTR_SIZE));
            size_t paramValueIndex{};
            for (PGParam const* const param: params) {
                managed->paramValues[paramValueIndex] = static_cast<char*>(calloc(param->value.size() + 1, CHAR_PTR_SIZE));
                memmove(managed->paramValues[paramValueIndex++], param->value.c_str(), param->value.size());
            }

            // delete/clear all the params
            for (PGParam* param: params) {
                delete param;
            }
            params.clear();

            // at this point the fields are ready to be passed to postgresql
            return managed;
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
        Builder& setSql(std::string const& sql) {
            if (managed->command != nullptr) {
                free(managed->command);
            }

            managed->command = static_cast<char*>(calloc(sql.size() + 1, sizeof(char)));
            memmove((void*)managed->command, sql.c_str(), sql.size());
            return *this;
        }

        /**
         * Adds a json[] param. The type param needs to have a toJson() method that returns the JSON.
         * @param value
         * @return
         */
        template <typename T>
        Builder& addJsonArrayParam(std::vector<T>&& value) requires toJson<T> {
            addParam(new PGJsonArray{std::move(value)});
            return *this;
        }

        /**
         * Adds a json[] param. The type param needs to have a writeJson(...) method that write the JSON object into the writer.
         * @param value
         * @return
         */
        template <typename T>
        Builder& addJsonArrayParam(std::vector<T>&& value) requires writeJson<T> {
            addParam(new PGJsonArray{std::move(value)});
            return *this;
        }

        /**
         * Adds a json[] param
         * @param value
         * @return
         */
        Builder& addJsonArrayParam(std::string&& value) {
            addParam(new PGJsonArray{std::move(value)});
            return *this;
        }

        /**
         * Adds a varchar param
         * @param value
         * @return
         */
        Builder& addParam(std::string&& value) {
            addParam(new PGVarchar{std::move(value)});
            return *this;
        }

        /**
         * Adds a varchar param
         * @param value
         * @return
         */
        Builder& addParam(std::string_view value) {
            addParam(new PGVarchar{std::string{value}});
            return *this;
        }

        /**
         * Adds a varchar param
         * @param value
         * @return
         */
        Builder& addParam(char const* value) {
            addParam(std::string_view{value});
            return *this;
        }

        /**
         * Adds a varchar param. This overload copies the string.
         * @param value
         * @return
         */
        Builder& addParam(std::string const& value) {
            addParam(new PGVarchar{std::string(value)});
            return *this;
        }

        /**
         * Adds an unsigned long param
         * @param value
         * @return
         */
        Builder& addParam(unsigned long value) {
            addParam(new PGBigUInt{value});
            return *this;
        }

        /**
         * Adds an int param
         * @param value
         * @return
         */
        Builder& addParam(int value) {
            addParam(new PGInt{value});
            return *this;
        }

        /**
         * Adds a double param
         * @param value
         * @return
         */
        Builder& addParam(double value) {
            addParam(new PGFloat{value});
            return *this;
        }

        /**
         * Adds a bool param
         * @param value
         * @return
         */
        Builder& addParam(bool value) {
            addParam(new PGBool{value});
            return *this;
        }

        /**
         * Adds an unsigned int param
         * @param value
         * @return
         */
        Builder& addParam(unsigned int value) {
            addParam(new PGUInt{value});
            return *this;
        }
    private:
        void addParam(PGParam* param) {
            managed->type = QUERY_WITH_PARAMS;
            params.emplace_back(param);
        }
    };
};

static PGQueryParams::Builder q(std::string&& sql) {
    return PGQueryParams::Builder::create(std::move(sql));
};

template <typename ...T>
static PGQueryParams *q(std::string&& sql, T&& ...t) {
    auto retVal = PGQueryParams::Builder::create(std::move(sql));
    (retVal.addParam(t), ...);
    return retVal.build();
};

#endif //PGQUEUE_PGQUERYPARAMS_HPP
