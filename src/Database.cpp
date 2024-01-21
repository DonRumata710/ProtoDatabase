#include <ProtoDatabase/Database.h>

#include <SQLiteCpp/Transaction.h>

#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/map_field.h>
#include <google/protobuf/repeated_ptr_field.h>

#include <proto/KeyOption.pb.h>


namespace ProtoDatabase
{

Database::Database() : database(":memory:", SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE)
{}

Database::Database(const std::string& path) : database(path, SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE)
{}

int64_t Database::getTableCount() const
{
    SQLite::Statement query(database, "SELECT COUNT(*) FROM sqlite_master WHERE type='table';");
    if (!query.executeStep())
        throw std::runtime_error("couldn't execute request to get table count");
    return query.getColumn(0);
}

std::unordered_set<std::string> Database::getTables() const
{
    std::unordered_set<std::string> res;
    SQLite::Statement query(database, "SELECT * FROM sqlite_master WHERE type='table';");
    while(query.executeStep())
    {
        res.emplace(query.getColumn(0).getString());
    }
    return res;
}

void Database::createTable(const google::protobuf::Message& message)
{
    createTable(message.GetDescriptor());
}

int64_t Database::insertMessage(const google::protobuf::Message& message)
{
    SQLite::Transaction transaction(database);

    uint64_t id = writeMessageImpl(message, false);
    transaction.commit();

    return id;
}

void Database::createTable(const google::protobuf::Descriptor* reflection)
{
    SQLite::Transaction transaction(database);
    createTableImpl(reflection);
    transaction.commit();
}

int64_t Database::writeMessage(const google::protobuf::Message& message)
{
    SQLite::Transaction transaction(database);

    uint64_t id = writeMessageImpl(message, true);
    transaction.commit();

    return id;
}

void Database::deleteMessage(const google::protobuf::Message& message)
{
    SQLite::Transaction transaction(database);
    deleteMessageImpl(message);
    transaction.commit();
}

void Database::clearTable(const std::string& type)
{
    SQLite::Transaction transaction(database);
    clearTableImpl(type);
    transaction.commit();
}

void Database::createTableImpl(const google::protobuf::Descriptor* descriptor, bool uniqueObjects)
{
    std::string fields;
    std::string uniqueFields;
    std::vector<std::pair<std::string, std::string>> foreignKeys;
    std::vector<std::string> fieldList;

    for (int i = 0; i < descriptor->field_count(); ++i)
    {
        const auto* field = descriptor->field(i);
        auto fieldName = getColumnName(field->name());

        if (field->is_map())
        {
            createMapTable(descriptor, field);
            continue;
        }

        if (field->is_repeated())
        {
            createArrayTable(descriptor, field);
            continue;
        }

        bool isKey = false;
        if (field->options().HasExtension(Proto::objectKeyField) && field->options().GetExtension(Proto::objectKeyField))
        {
            uniqueFields += ",UNIQUE(" + fieldName + ')';
            isKey = true;
            uniqueObjects = true;
        }

        if (field->cpp_type() == google::protobuf::FieldDescriptor::CppType::CPPTYPE_MESSAGE)
        {
            auto nestedMessage = field->message_type();
            createTableImpl(nestedMessage, isKey);
            foreignKeys.emplace_back(fieldName, nestedMessage->name());
        }

        fields += ',' + fieldName + " " + getFieldType(field);
        fieldList.emplace_back(fieldName);
    }

    if (uniqueFields.empty() && uniqueObjects)
    {
        uniqueFields += ",UNIQUE(";
        for (int i = 0; i < fieldList.size(); ++i)
        {
            auto fieldName = fieldList[i];
            if (i > 0)
                uniqueFields += ',';
            uniqueFields += fieldName;
        }
        uniqueFields += ')';
    }

    if (!foreignKeys.empty())
    {
        for (const auto& foreignKey : foreignKeys)
            fields += ",FOREIGN KEY(" + foreignKey.first + ") REFERENCES " + foreignKey.second + "(id)";
    }

    std::string fullSQL = "CREATE TABLE IF NOT EXISTS " + descriptor->name() + " (id INTEGER PRIMARY KEY" + fields + uniqueFields + ");";

    database.exec(fullSQL);

    for (const auto& key : foreignKeys)
    {
        SQLite::Statement trigger(database,
                                  "CREATE TRIGGER IF NOT EXISTS on_delete_" + descriptor->name() + "_" + key.second + " AFTER DELETE ON " + descriptor->name() + " BEGIN"
                                  "  DELETE FROM " + key.second + " WHERE id = old." + key.first + ";"
                                  "END;");
        trigger.exec();
    }
}

int64_t Database::writeMessageImpl(const google::protobuf::Message& message, bool handleConficts) const
{
    std::string fieldNames;
    std::string fieldValues;
    std::string excludedValues;
    std::vector<const google::protobuf::FieldDescriptor*> repeatedFields;
    std::vector<const google::protobuf::FieldDescriptor*> dataFields;
    std::vector<const google::protobuf::FieldDescriptor*> mapFields;
    for (int i = 0; i < message.GetDescriptor()->field_count(); ++i)
    {
        const auto* field = message.GetDescriptor()->field(i);

        if (field->is_map())
        {
            mapFields.emplace_back(field);
            continue;
        }

        if (field->is_repeated())
        {
            repeatedFields.emplace_back(field);
            continue;
        }

        dataFields.emplace_back(field);

        if (!fieldNames.empty())
        {
            fieldNames += ", ";
            fieldValues += ", ";
            excludedValues += ", ";
        }

        auto fieldName = getColumnName(field->name());
        fieldNames += fieldName;
        fieldValues += "?";
        excludedValues += fieldName + "=excluded." + fieldName;
    }

    std::string fullSQL = "INSERT INTO " + message.GetDescriptor()->name();
    if (!fieldNames.empty())
    {
        fullSQL += " (" + fieldNames + ") VALUES (" + fieldValues + ")";
        if (handleConficts)
            fullSQL += " ON CONFLICT DO UPDATE SET " + excludedValues;
    }
    else
    {
        fullSQL += " DEFAULT VALUES";
    }

    fullSQL += ';';

    SQLite::Statement query(database, fullSQL);
    insertMessageFields(query, message, dataFields, true);

    if (query.exec() == 0)
        throw std::runtime_error("couldn't write message to the database");

    auto id = database.getLastInsertRowid();

    for (const auto* field : mapFields)
        writeMap(message, field, id);

    for (const auto* field : repeatedFields)
        writeArray(message, field, id);

    return id;
}

void Database::deleteMessageImpl(const google::protobuf::Message& message)
{
    auto keys = getMessageKeys(message);

    if (keys.empty())
        throw std::logic_error("no keys for deletion of " + message.GetDescriptor()->name());

    std::string fullSQL = "DELETE FROM " + message.GetDescriptor()->name() + " WHERE ";
    for (size_t i = 0; i < keys.size(); ++i)
    {
        fullSQL += keys[i]->name() + "=?";
        if (i < keys.size() - 1)
            fullSQL = " AND ";
    }
    fullSQL += ';';

    SQLite::Statement query(database, fullSQL);
    insertMessageFields(query, message, keys, false);

    database.exec(fullSQL);
}

void Database::createMapTable(const google::protobuf::Descriptor* descriptor, const google::protobuf::FieldDescriptor* field)
{
    if (!field->message_type() || !field->message_type()->map_key())
        throw std::logic_error("not message type in map table creation");

    auto keyField = field->message_type()->map_key();
    std::string keyFieldName = getColumnName(keyField->name());
    auto valueField = field->message_type()->map_value();
    std::string valueFieldName = getColumnName(valueField->name());
    std::string fullSQL = "CREATE TABLE IF NOT EXISTS " + getFieldTableName(descriptor, field) + " (id INTEGER PRIMARY KEY, " +
                                keyFieldName + " " + getFieldType(keyField) + ", " +
                                valueFieldName + " " + getFieldType(valueField) + ", "
                                "owner_id INTEGER, "
                                "UNIQUE(owner_id, " + keyFieldName + "),"
                                "FOREIGN KEY(owner_id) REFERENCES " + descriptor->name() + "(id)";

    if (valueField->cpp_type() == google::protobuf::FieldDescriptor::CppType::CPPTYPE_MESSAGE)
    {
        auto typeDesc = valueField->message_type();
        createTableImpl(typeDesc);
        fullSQL += ", FOREIGN KEY(" + valueFieldName + ") REFERENCES " + typeDesc->name() + "(id)";
    }
    fullSQL += ");";

    database.exec(fullSQL);
}

void Database::createArrayTable(const google::protobuf::Descriptor* descriptor, const google::protobuf::FieldDescriptor* field)
{
    std::string fieldName = getColumnName(field->name());
    std::string fullSQL = "CREATE TABLE IF NOT EXISTS " + getFieldTableName(descriptor, field) + " ("
                                "id INTEGER PRIMARY KEY," + fieldName + " " + getFieldType(field) + ", owner_id INTEGER,"
                                "UNIQUE(owner_id," + fieldName + "),"
                                "FOREIGN KEY(owner_id) REFERENCES " + descriptor->name() + "(id)";

    if (field->cpp_type() == google::protobuf::FieldDescriptor::CppType::CPPTYPE_MESSAGE)
    {
        createTableImpl(field->message_type());
        fullSQL += ", FOREIGN KEY(" + fieldName + ") REFERENCES " + field->message_type()->name() + "(id)";
    }
    fullSQL += ");";

    database.exec(fullSQL);
}

bool Database::isKey(const google::protobuf::FieldDescriptor* field)
{
    return field->options().HasExtension(Proto::objectKeyField) && field->options().GetExtension(Proto::objectKeyField);
}

SQLite::Statement Database::getAllObjects(const std::string& type) const
{
    return SQLite::Statement{ database, "SELECT * FROM " + type + ';' };
}

std::optional<int64_t> Database::findMessage(const google::protobuf::Message& message) const
{
    auto keys = getMessageKeys(message);

    std::string queryStr = "SELECT id FROM " + message.GetDescriptor()->name();
    if (!keys.empty())
    {
        std::string condition;
        for (size_t i = 0; i < keys.size(); ++i)
        {
            condition += getColumnName(keys[i]->name()) + "=?";
            if (i < keys.size() - 1)
                condition += " AND ";
        }
        queryStr += " WHERE " + std::move(condition);
    }
    queryStr += " ORDER BY id;";

    SQLite::Statement query(database, queryStr);
    insertMessageFields(query, message, keys, false);

    if (!query.executeStep())
        return std::optional<int64_t>{};

    return query.getColumn(0).getInt64();
}

void Database::findMessage(const std::string& type, int64_t id, google::protobuf::Message* message) const
{
    SQLite::Statement query{ database, "SELECT * FROM " + type + " WHERE id=" + std::to_string(id) + ';' };
    if (!query.executeStep())
        throw std::logic_error("couldn't find object with type " + type + " and ID " + std::to_string(id));

    readFields(query, message);
}

std::vector<int64_t> Database::findAssociatedMessages(const std::string& type, int64_t owner) const
{
    SQLite::Statement query{ database, "SELECT id FROM " + type + " WHERE owner_id=" + std::to_string(owner) + " ORDER BY id;" };
    std::vector<int64_t> res;
    while(query.executeStep())
        res.emplace_back(query.getColumn(0).getInt64());
    return res;
}

void Database::readFields(SQLite::Statement& query, google::protobuf::Message* message) const
{
    for (int column = 1, fieldIndex = 0; fieldIndex < message->GetDescriptor()->field_count(); ++fieldIndex)
    {
        auto field = message->GetDescriptor()->field(fieldIndex);
        if (field->is_repeated() && !field->is_map())
        {
            std::string tableName = getFieldTableName(message->GetDescriptor(), field);
            auto messages = findAssociatedMessages(tableName, query.getColumn(0));
            for (auto msgId : messages)
            {
                std::string columnName = getColumnName(field->name());
                SQLite::Statement arrayQuery{ database, "SELECT " + columnName + " FROM " + tableName + " WHERE id=" + std::to_string(msgId) };
                if (!arrayQuery.executeStep())
                    throw std::runtime_error("couldn't find object with ID " + std::to_string(msgId) + " in table " + tableName);

                switch(field->cpp_type())
                {
                case google::protobuf::FieldDescriptor::CPPTYPE_INT32:
                    message->GetReflection()->AddInt32(message, field, arrayQuery.getColumn(0));
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_INT64:
                    message->GetReflection()->AddInt64(message, field, arrayQuery.getColumn(0));
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_UINT32:
                    message->GetReflection()->AddUInt32(message, field, arrayQuery.getColumn(0));
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
                    message->GetReflection()->AddUInt64(message, field, atoi(arrayQuery.getColumn(0).getString().c_str()));
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
                    message->GetReflection()->AddDouble(message, field, arrayQuery.getColumn(0));
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT:
                    message->GetReflection()->AddFloat(message, field, static_cast<double>(arrayQuery.getColumn(0)));
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_BOOL:
                    message->GetReflection()->AddBool(message, field, static_cast<int>(arrayQuery.getColumn(0)));
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_ENUM:
                    message->GetReflection()->AddEnumValue(message, field, arrayQuery.getColumn(0));
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_STRING:
                    message->GetReflection()->AddString(message, field, arrayQuery.getColumn(0));
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE:
                {
                    auto nestedMessage = message->GetReflection()->AddMessage(message, field);
                    findMessage(nestedMessage->GetDescriptor()->name(), arrayQuery.getColumn(0), nestedMessage);
                    break;
                }
                default:
                    throw std::logic_error(std::string{ "unexpected field type: " } + field->cpp_type_name());
                }
            }
        }
        else
        {
            switch(field->cpp_type())
            {
            case google::protobuf::FieldDescriptor::CppType::CPPTYPE_STRING:
                message->GetReflection()->SetString(message, field, query.getColumn(column));
                break;
            case google::protobuf::FieldDescriptor::CppType::CPPTYPE_INT32:
                message->GetReflection()->SetInt32(message, field, query.getColumn(column));
                break;
            case google::protobuf::FieldDescriptor::CppType::CPPTYPE_INT64:
                message->GetReflection()->SetInt64(message, field, query.getColumn(column));
                break;
            case google::protobuf::FieldDescriptor::CppType::CPPTYPE_UINT32:
                message->GetReflection()->SetUInt32(message, field, query.getColumn(column));
                break;
            case google::protobuf::FieldDescriptor::CppType::CPPTYPE_UINT64:
                message->GetReflection()->SetUInt64(message, field, atoi(query.getColumn(column).getString().c_str()));
                break;
            case google::protobuf::FieldDescriptor::CppType::CPPTYPE_BOOL:
                message->GetReflection()->SetBool(message, field, (static_cast<int>(query.getColumn(column)) != 0 ? true : false));
                break;
            case google::protobuf::FieldDescriptor::CppType::CPPTYPE_DOUBLE:
                message->GetReflection()->SetDouble(message, field, query.getColumn(column));
                break;
            case google::protobuf::FieldDescriptor::CppType::CPPTYPE_FLOAT:
                message->GetReflection()->SetFloat(message, field, static_cast<double>(query.getColumn(column)));
                break;
            case google::protobuf::FieldDescriptor::CppType::CPPTYPE_ENUM:
                message->GetReflection()->SetEnumValue(message, field, query.getColumn(column));
                break;
            case google::protobuf::FieldDescriptor::CppType::CPPTYPE_MESSAGE:
                if (field->is_map())
                {
                    auto tableName = getFieldTableName(message->GetDescriptor(), field);
                    auto messages = findAssociatedMessages(tableName, query.getColumn(0));
                    for (auto msgId : messages)
                    {
                        auto nestedMessage = message->GetReflection()->AddMessage(message, field);
                        findMessage(tableName, msgId, nestedMessage);
                    }
                    continue;
                }
                else
                {
                    auto nestedMessage = message->GetReflection()->MutableMessage(message, field);
                    findMessage(nestedMessage->GetDescriptor()->name(), query.getColumn(column).getInt64(), nestedMessage);
                }
                break;
            }

            ++column;
        }
    }
}

std::string Database::getFieldType(const google::protobuf::FieldDescriptor* field)
{
    std::string type;
    switch (field->cpp_type())
    {
    case google::protobuf::FieldDescriptor::CPPTYPE_INT32:
    case google::protobuf::FieldDescriptor::CPPTYPE_INT64:
    case google::protobuf::FieldDescriptor::CPPTYPE_UINT32:
    case google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
        type = "INTEGER";
        break;
    case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
    case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT:
        type = "REAL";
        break;
    case google::protobuf::FieldDescriptor::CPPTYPE_BOOL:
        type = "BOOLEAN";
        break;
    case google::protobuf::FieldDescriptor::CPPTYPE_ENUM:
        type = "INTEGER";
        break;
    case google::protobuf::FieldDescriptor::CPPTYPE_STRING:
        type = "LONGTEXT";
        break;
    case google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE:
        type = "INTEGER";
        break;
    }
    return type;
}

std::vector<const google::protobuf::FieldDescriptor*> Database::getMessageKeys(const google::protobuf::Message& message, bool strict) const
{
    std::vector<const google::protobuf::FieldDescriptor*> res;

    for (int i = 0; i < message.GetDescriptor()->field_count(); ++i)
    {
        const auto& field = message.GetDescriptor()->field(i);
        if (field->options().HasExtension(Proto::objectKeyField))
        {
            if (field->options().GetExtension(Proto::objectKeyField))
            {
                if (!message.GetReflection()->HasField(message, field))
                    throw std::runtime_error("No key value in key field for " + message.GetTypeName() + " message");

                res.emplace_back(field);
            }
        }
    }

    if (res.empty() && !strict)
    {
        for (int i = 0; i < message.GetDescriptor()->field_count(); ++i)
            res.emplace_back(message.GetDescriptor()->field(i));
    }

    return res;
}

void Database::insertMessageFields(SQLite::Statement& query, const google::protobuf::Message& message, const std::vector<const google::protobuf::FieldDescriptor*>& fields, bool isInsertion, size_t offset) const
{
    for (size_t i = 1; i <= fields.size(); ++i)
    {
        auto field = fields[i - 1];

        if (field->is_repeated() || field->is_map())
            throw std::logic_error("couldn't insert inappropriate field to the table");

        switch (field->cpp_type())
        {
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_STRING:
            query.bind(i + offset, message.GetReflection()->GetString(message, field));
            break;
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_INT32:
            query.bind(i + offset, message.GetReflection()->GetInt32(message, field));
            break;
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_INT64:
            query.bind(i + offset, message.GetReflection()->GetInt64(message, field));
            break;
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_UINT32:
            query.bind(i + offset, message.GetReflection()->GetUInt32(message, field));
            break;
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_UINT64:
            query.bind(i + offset, std::to_string(message.GetReflection()->GetUInt64(message, field)));
            break;
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_BOOL:
            query.bind(i + offset, message.GetReflection()->GetBool(message, field) ? 1 : 0);
            break;
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_DOUBLE:
            query.bind(i + offset, message.GetReflection()->GetDouble(message, field));
            break;
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_FLOAT:
            query.bind(i + offset, message.GetReflection()->GetFloat(message, field));
            break;
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_ENUM:
            query.bind(i + offset, message.GetReflection()->GetEnumValue(message, field));
            break;
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_MESSAGE:
        {
            std::optional<int64_t> key;
            if (isInsertion)
                key = writeMessageImpl(message.GetReflection()->GetMessage(message, field), false);
            else
                key = findMessage(message.GetReflection()->GetMessage(message, field));

            if (!key)
                throw std::runtime_error("No nested object in " + message.GetTypeName() + " message");

            query.bind(i + offset, key.value());
            break;
        }
        default:
            throw std::logic_error(std::string("Unsupported field type: ") + field->cpp_type_name());
        }
    }
}

void Database::insertMessageRepeatedField(SQLite::Statement& query, const google::protobuf::Message& message, const google::protobuf::FieldDescriptor* field, bool isInsertion) const
{
    switch (field->cpp_type())
    {
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_STRING:
        for (size_t i = 1; i <= message.GetReflection()->FieldSize(message, field); ++i)
            query.bind(i, message.GetReflection()->GetRepeatedString(message, field, i - 1));
        break;
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_INT32:
        for (size_t i = 1; i <= message.GetReflection()->FieldSize(message, field); ++i)
            query.bind(i, message.GetReflection()->GetRepeatedInt32(message, field, i - 1));
        break;
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_INT64:
        for (size_t i = 1; i <= message.GetReflection()->FieldSize(message, field); ++i)
            query.bind(i, message.GetReflection()->GetRepeatedInt64(message, field, i - 1));
        break;
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_UINT32:
        for (size_t i = 1; i <= message.GetReflection()->FieldSize(message, field); ++i)
            query.bind(i, message.GetReflection()->GetRepeatedUInt32(message, field, i - 1));
        break;
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_UINT64:
        for (size_t i = 1; i <= message.GetReflection()->FieldSize(message, field); ++i)
            query.bind(i, std::to_string(message.GetReflection()->GetRepeatedUInt64(message, field, i - 1)));
        break;
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_BOOL:
        for (size_t i = 1; i <= message.GetReflection()->FieldSize(message, field); ++i)
            query.bind(i, message.GetReflection()->GetRepeatedBool(message, field, i - 1) ? 1 : 0);
        break;
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_DOUBLE:
        for (size_t i = 1; i <= message.GetReflection()->FieldSize(message, field); ++i)
            query.bind(i, message.GetReflection()->GetRepeatedDouble(message, field, i - 1));
        break;
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_FLOAT:
        for (size_t i = 1; i <= message.GetReflection()->FieldSize(message, field); ++i)
            query.bind(i, message.GetReflection()->GetRepeatedFloat(message, field, i - 1));
        break;
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_ENUM:
        for (size_t i = 1; i <= message.GetReflection()->FieldSize(message, field); ++i)
            query.bind(i, message.GetReflection()->GetRepeatedEnumValue(message, field, i - 1));
        break;
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_MESSAGE:
    {
        for (size_t i = 1; i <= message.GetReflection()->FieldSize(message, field); ++i)
        {
            std::optional<int64_t> key;
            if (isInsertion)
                key = writeMessageImpl(message.GetReflection()->GetRepeatedMessage(message, field, i - 1), false);
            else
                key = findMessage(message.GetReflection()->GetRepeatedMessage(message, field, i - 1));

            if (!key)
                throw std::runtime_error("No nested object in " + message.GetTypeName() + " message");

            query.bind(i, key.value());
        }
        break;
    }
    default:
        throw std::logic_error(std::string("Unsupported field type: ") + field->cpp_type_name());
    }
}

void Database::writeMap(const google::protobuf::Message& message, const google::protobuf::FieldDescriptor* field, int64_t id) const
{
    removeMap(message, field, id);

    std::string idStr = std::to_string(id);
    std::string values;

    auto arraySize = message.GetReflection()->FieldSize(message, field);
    if (arraySize == 0)
        return;

    for (int i = 0; i < arraySize; ++i)
    {
        if (!values.empty())
            values += ',';
        values += "(?,?," + idStr + ")";
    }

    std::string fullSQL = "INSERT INTO " + getFieldTableName(message.GetDescriptor(), field) + "(" +
                            getColumnName(field->message_type()->map_key()->name()) + ", " +
                            getColumnName(field->message_type()->map_value()->name()) + ", "
                            "owner_id) "
                          "VALUES " + values + ";";
    SQLite::Statement query(database, fullSQL);
    std::vector<const google::protobuf::FieldDescriptor*> fields{ field->message_type()->map_key(), field->message_type()->map_value() };

    for (auto i = 0; i < arraySize; ++i)
        insertMessageFields(query, message.GetReflection()->GetRepeatedMessage(message, field, i), fields, true, i * 2);

    if (query.exec() != arraySize)
        throw std::runtime_error("couldn't insert array values to " + getFieldTableName(message.GetDescriptor(), field));
}

void Database::removeMap(const google::protobuf::Message& message, const google::protobuf::FieldDescriptor* field, int64_t id) const
{
    std::string fullSQL = "DELETE FROM " + getFieldTableName(message.GetDescriptor(), field) + " WHERE owner_id=" + std::to_string(id);
    SQLite::Statement query(database, fullSQL);
    query.exec();
}

void Database::writeArray(const google::protobuf::Message& message, const google::protobuf::FieldDescriptor* field, int64_t id) const
{
    removeArray(message, field, id);

    std::string idStr = std::to_string(id);

    auto arraySize = message.GetReflection()->FieldSize(message, field);
    if (arraySize == 0)
        return;

    std::string values;
    for (int i = 0; i < arraySize; ++i)
    {
        if (!values.empty())
            values += ',';
        values += "(?," + idStr + ")";
    }

    std::string fullSQL = "INSERT INTO " + getFieldTableName(message.GetDescriptor(), field) + "(" + getColumnName(field->name()) + ",owner_id) "
                          "VALUES " + values + ";";
    SQLite::Statement query(database, fullSQL);

    insertMessageRepeatedField(query, message, field, true);

    if (query.exec() != arraySize)
        throw std::runtime_error("couldn't insert array values to " + getFieldTableName(message.GetDescriptor(), field));
}

void Database::removeArray(const google::protobuf::Message& message, const google::protobuf::FieldDescriptor* field, int64_t id) const
{
    std::string fullSQL = "DELETE FROM " + getFieldTableName(message.GetDescriptor(), field) + " WHERE owner_id=" + std::to_string(id);
    SQLite::Statement query(database, fullSQL);
    query.exec();
}

void Database::clearTableImpl(const std::string& type)
{
    database.exec("DELETE FROM " + type);
}

std::string Database::getFieldTableName(const google::protobuf::Descriptor* descriptor, const google::protobuf::FieldDescriptor* field)
{
    return "field_table_" + descriptor->name() + "_" + field->name();
}

std::string Database::getColumnName(const std::string& fieldName)
{
    return "field_" + fieldName;
}

}
