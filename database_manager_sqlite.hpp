#pragma once
#include <database_manager.hpp>
#include <sqlite3.h>
#include <unordered_map>
#include <functional>
#include <vector>

class DatabaseManagerSqlite : public DatabaseManager {
public:
    DatabaseManagerSqlite();
    ~DatabaseManagerSqlite();

    virtual void createTable(const std::string& tableName,const std::vector<std::tuple<std::string, std::string, std::string>>& columns, const std::vector<std::string>& tableConstraints = {}) override;
    bool insertRecord(const std::string& tableName, const std::vector<std::tuple<std::string, std::string, SQLType>>& columnValuePairs, std::string& errorMsg) override;
    std::vector<std::vector<std::string>> getAllRecords(const std::string& tableName) override;
    std::vector<std::vector<std::string>> getRecordByCondition(const std::string& tableName, const std::string& conditionColumn, const std::string& conditionValue) override;
    std::vector<std::vector<std::string>> getRecordsByFieldValueBetween(const std::string& tableName, const std::string& fieldName, const std::string& fromValue, const std::string& toValue) override;
    std::vector<std::vector<std::string>> getColumns(const std::string& tableName, const std::vector<std::string>& columnNames) override;
    bool deleteRecord(const std::string& tableName, const std::string& condition, std::string& errorMsg) override;
    std::vector<std::vector<std::string>> getRecordsInRange(const std::string& tableName, const std::string& sortColumn, int start, int end) override;
    int getTotalRecords(const std::string& tableName) override;
    bool deleteTable(const std::string& tableName, std::string& errorMsg) override;
    bool updateRecord(const std::string& tableName, const std::string& conditionColumn, const std::pair<std::string, SQLType>& conditionValue, const std::vector<std::tuple<std::string, std::string, SQLType>>& columnValuePairs, std::string& errorMsg) override;
private:
    sqlite3* db;
    bool prepareAndExecute(const std::string& sql, const std::vector<std::pair<std::string, SQLType>>& values, std::string& errorMsg, std::function<void(sqlite3_stmt*)> rowCallback = nullptr);
    static int callback(void* data, int argc, char** argv, char** azColName);
};
