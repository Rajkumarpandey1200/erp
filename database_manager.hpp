#ifndef DATABASE_MANAGER_HPP
#define DATABASE_MANAGER_HPP

#include <vector>
#include <string>
#include <tuple>
#include <utility> 

enum class SQLType {
    STRING,
    INTEGER,
    BOOLEAN
};

class DatabaseManager {
public:
    virtual ~DatabaseManager() = default;

    virtual void createTable(const std::string& tableName,const std::vector<std::tuple<std::string, std::string, std::string>>& columns,const std::vector<std::string>& tableConstraints = {}) = 0;
    virtual bool insertRecord(const std::string& tableName, const std::vector<std::tuple<std::string, std::string, SQLType>>& columnValuePairs,  std::string& errorMsg) = 0;
    virtual std::vector<std::vector<std::string>> getAllRecords(const std::string& tableName) = 0;
    virtual std::vector<std::vector<std::string>> getRecordByCondition(const std::string& tableName, const std::string& conditionColumn, const std::string& conditionValue) = 0;
    virtual std::vector<std::vector<std::string>> getRecordsByFieldValueBetween(const std::string& tableName, const std::string& fieldName, const std::string& fromValue, const std::string& toValue) = 0;
    virtual std::vector<std::vector<std::string>> getColumns(const std::string& tableName, const std::vector<std::string>& columnNames) = 0;
    virtual bool deleteRecord(const std::string& tableName, const std::string& condition, std::string& errorMsg) = 0;
    virtual int getTotalRecords(const std::string& tableName) = 0;
    virtual bool deleteTable(const std::string& tableName, std::string& errorMsg) = 0;
    virtual std::vector<std::vector<std::string>> getRecordsInRange(const std::string& tableName, const std::string& sortColumn, int start, int end) = 0;

    virtual bool updateRecord(const std::string& tableName, const std::string& conditionColumn, const std::pair<std::string, SQLType>& conditionValue, const std::vector<std::tuple<std::string, std::string, SQLType>>& columnValuePairs, std::string& errorMsg) = 0;
};

#endif 
