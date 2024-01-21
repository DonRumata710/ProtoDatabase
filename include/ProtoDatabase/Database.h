#pragma once

#include <SQLiteCpp/Database.h>

#include <google/protobuf/message.h>

#include <string>
#include <unordered_set>


namespace ProtoDatabase
{

class EXPORT_ProtoDatabase Database
{
public:
    Database();
    Database(const std::string& path);

    /**
     * @brief getTableCount
     * @return number of tables in the database
     */
    int64_t getTableCount() const;

    /**
     * @brief getTables
     * @return list of tables in the database
     */
    std::unordered_set<std::string> getTables() const;

    /**
     * @brief createTable
     *
     * creates table for T message
     */
    template<typename T>
    void createTable()
    {
        createTable(T::GetDescriptor());
    }

    /**
     * @brief createTable
     *
     * creates table for the type of message
     */
    void createTable(const google::protobuf::Message& message);

    /**
     * @brief insertMessage
     *
     * Creates a new row with data from the message or throws an exception if there will be conflicts with unique keys
     *
     * @param message - object to write into the database
     * @return ID of inserted row
     */
    int64_t insertMessage(const google::protobuf::Message& message);

    /**
     * @brief writeMessage
     *
     * Creates a new row with data from the message or update an old row if there will be conflicts with unique keys
     *
     * @param message - object to write into the database
     * @return ID of inserted row
     */
    int64_t writeMessage(const google::protobuf::Message& message);

    /**
     * @brief findMessage
     * @param field - key field
     * @param key - value for search
     * @return found message or empty optional
     */
    template<typename Message, typename Key>
    std::optional<Message> findMessage(const google::protobuf::FieldDescriptor* field, const Key& key)
    {
        if (!isKey(field))
            throw std::logic_error("field is not a key for " + Message::GetDescriptor()->name());

        SQLite::Statement query{ database, "SELECT * FROM " + Message::GetDescriptor()->name() + " WHERE " + getColumnName(field->name()) + "=?;" };
        if constexpr(std::is_base_of<google::protobuf::Message, Key>::value)
        {
            auto keyId = findMessage(key);
            if (!keyId)
                return std::optional<Message>{};
            query.bind(1, keyId.value());
        }
        else
        {
            query.bind(1, key);
        }

        if (!query.executeStep())
            return std::optional<Message>{};

        Message message;
        readFields(query, &message);
        return message;
    }

    /**
     * @brief getAllMessages
     * @return all messages of selected type
     */
    template<typename Message>
    std::vector<Message> getAllMessages() const
    {
        std::vector<Message> res;
        auto query = getAllObjects(Message::GetDescriptor()->name());
        while(query.executeStep())
        {
            Message message;
            readFields(query, &message);
            res.emplace_back(std::move(message));
        }
        return res;
    }

    /**
     * @brief deleteMessage
     * @param field - key field
     * @param key - value for search
     *
     * Removes objects found by specified key
     */
    template<typename Message, typename Key>
    void deleteMessage(const google::protobuf::FieldDescriptor* field, const Key& key)
    {
        if (!isKey(field))
            throw std::logic_error("field is not a key for " + Message::GetDescriptor()->name());

        SQLite::Statement query{ database, "DELETE FROM " + Message::GetDescriptor()->name() + " WHERE " + getColumnName(field->name()) + "=?;" };
        if constexpr(std::is_base_of<google::protobuf::Message, Key>::value)
        {
            auto keyId = findMessage(key);
            if (!keyId)
                return;
            query.bind(1, keyId.value());
        }
        else
        {
            query.bind(1, key);
        }

        query.exec();
    }

    /**
     * @brief deleteMessage
     * @param message - object to be deleted
     *
     * Removes specified object from the table
     */
    void deleteMessage(const google::protobuf::Message& message);

    /**
     * @brief clearTable
     * @param type - type of messages to be deleted
     */
    void clearTable(const std::string& type);

    /**
     * @brief clearTable
     *
     * Removes all rows in the table of specified type
     */
    template<typename T>
    void clearTable()
    {
        clearTable(T::GetDescriptor()->name());
    }

private:
    void createTable(const google::protobuf::Descriptor* reflection);
    void createTableImpl(const google::protobuf::Descriptor* reflection, bool uniqueObjects = false);

    int64_t writeMessageImpl(const google::protobuf::Message& message, bool handleConficts) const;

    void deleteMessageImpl(const google::protobuf::Message& message);

    void createMapTable(const google::protobuf::Descriptor*, const google::protobuf::FieldDescriptor* field);
    void createArrayTable(const google::protobuf::Descriptor*, const google::protobuf::FieldDescriptor* field);

    bool isKey(const google::protobuf::FieldDescriptor* field);

    SQLite::Statement getAllObjects(const std::string& type) const;
    std::optional<int64_t> findMessage(const google::protobuf::Message& message) const;

    void findMessage(const std::string& type, int64_t id, google::protobuf::Message* message) const;
    std::vector<int64_t> findAssociatedMessages(const std::string& type, int64_t owner) const;

    void readFields(SQLite::Statement& query, google::protobuf::Message* message) const;

    std::string getFieldType(const google::protobuf::FieldDescriptor* field);

    std::vector<const google::protobuf::FieldDescriptor*> getMessageKeys(const google::protobuf::Message& message, bool strict = false) const;
    void insertMessageFields(SQLite::Statement& query, const google::protobuf::Message& message, const std::vector<const google::protobuf::FieldDescriptor*>& fields, bool isInsertion, size_t offset = 0) const;
    void insertMessageRepeatedField(SQLite::Statement& query, const google::protobuf::Message& message, const google::protobuf::FieldDescriptor* field, bool isInsertion) const;

    void writeMap(const google::protobuf::Message& message, const google::protobuf::FieldDescriptor* field, int64_t id) const;
    void removeMap(const google::protobuf::Message& message, const google::protobuf::FieldDescriptor* field, int64_t id) const;

    void writeArray(const google::protobuf::Message& message, const google::protobuf::FieldDescriptor* field, int64_t id) const;
    void removeArray(const google::protobuf::Message& message, const google::protobuf::FieldDescriptor* field, int64_t id) const;

    std::string getFieldStringValue(const google::protobuf::Message& message, const google::protobuf::FieldDescriptor* field);

    void clearTableImpl(const std::string& type);

    static std::string getFieldTableName(const google::protobuf::Descriptor*, const google::protobuf::FieldDescriptor* field);
    static std::string getColumnName(const std::string& fieldName);

private:
    SQLite::Database database;
};

}
