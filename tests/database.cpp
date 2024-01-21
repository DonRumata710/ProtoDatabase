#include <catch2/catch_all.hpp>

#include <ProtoDatabase/Database.h>

#include <google/protobuf/util/message_differencer.h>

#include "proto/messages.pb.h"
#include "proto/messages.pb.cc"


using namespace ProtoDatabase;


TEST_CASE("Database test", "[smoketest]") {
    Database db;

    REQUIRE_NOTHROW(db.createTable<TestMessage>());

    REQUIRE(db.getTableCount() == 2);
}

TEST_CASE("Message store test", "[smoketest]") {
    Database db;

    const std::string testString = "test string";
    const int value1 = 42;
    const int value2 = 23;

    REQUIRE_NOTHROW(db.createTable<TestMessage>());

    {
        TestMessage message;
        message.set_enumvalue(TestMessage::TestEnum::TestMessage_TestEnum_val2);
        message.set_stringvalue(testString);
        message.set_value(value1);

        TestMessage::TestNestedMessage nestedMessage;
        nestedMessage.set_value(23);

        *message.mutable_nestedmessage() = nestedMessage;

        REQUIRE_NOTHROW(db.writeMessage(message));
    }

    {
        std::vector<TestMessage> messages;
        REQUIRE_NOTHROW(messages = db.getAllMessages<TestMessage>());
        REQUIRE(messages.size() == 1);

        REQUIRE(messages[0].enumvalue() == TestMessage::TestEnum::TestMessage_TestEnum_val2);
        REQUIRE(messages[0].stringvalue() == testString);
        REQUIRE(messages[0].value() == value1);
        REQUIRE(messages[0].nestedmessage().value() == value2);
    }

    {
        REQUIRE_NOTHROW(db.clearTable<TestMessage>());
        std::vector<TestMessage> messages;
        REQUIRE_NOTHROW(messages = db.getAllMessages<TestMessage>());
        REQUIRE(messages.empty());
    }
}

TEST_CASE("Repeated message store test", "[smoketest]") {
    Database db;

    const std::string testString1 = "test string";
    const std::string testString2 = "another test string";
    const int value1 = 42;
    const int value2 = 23;

    REQUIRE_NOTHROW(db.createTable<TestRepeated>());

    {
        TestRepeated message;

        TestRepeated::TestMessage repeated1;
        repeated1.set_intvalue(value1);
        repeated1.set_strvalue(testString1);
        message.mutable_msg()->Add(std::move(repeated1));

        TestRepeated::TestMessage repeated2;
        repeated2.set_intvalue(value2);
        repeated2.set_strvalue(testString2);
        message.mutable_msg()->Add(std::move(repeated2));

        REQUIRE_NOTHROW(db.writeMessage(message));
    }

    {
        std::vector<TestRepeated> msgList;
        REQUIRE_NOTHROW(msgList = db.getAllMessages<TestRepeated>());

        REQUIRE(msgList.size() == 1);
        REQUIRE(msgList[0].msg_size() == 2);
        REQUIRE(msgList[0].msg()[0].intvalue() == value1);
        REQUIRE(msgList[0].msg()[0].strvalue() == testString1);
        REQUIRE(msgList[0].msg()[1].intvalue() == value2);
        REQUIRE(msgList[0].msg()[1].strvalue() == testString2);
    }
}

TEST_CASE("Map store test", "[smoketest]") {
    Database db;

    const std::string testString1 = "test string";
    const std::string testString2 = "another test string";
    const int value1 = 42;
    const int value2 = 23;

    REQUIRE_NOTHROW(db.createTable<TestMap>());

    {
        TestMap msg;
        msg.mutable_data()->emplace(testString1, value1);
        msg.mutable_data()->emplace(testString2, value2);

        REQUIRE_NOTHROW(db.writeMessage(msg));
    }

    {
        std::vector<TestMap> messages;
        REQUIRE_NOTHROW(messages = db.getAllMessages<TestMap>());
        REQUIRE(messages.size() == 1);
        REQUIRE(messages[0].data_size() == 2);
        REQUIRE(messages[0].data().at(testString1) == value1);
        REQUIRE(messages[0].data().at(testString2) == value2);
    }
}

char get_rand_char()
{
    static const std::string charset("qwertyuiopasdfghjklzxcvbnm1234567890");
    return charset[rand() % charset.size()];
}

std::string generate_random_string(size_t n) {
    std::string res;
    res.reserve(n);
    for (size_t i = 0; i < n; ++i)
        res.push_back(get_rand_char());
    return res;
}

void EqualMessages(const google::protobuf::Message& first, const google::protobuf::Message& second)
{
    if (first.GetDescriptor() != second.GetDescriptor())
        throw std::runtime_error("messages have different types");

    for (int i = 0; i < first.GetDescriptor()->field_count(); ++i)
    {
        auto field = first.GetDescriptor()->field(i);

        if (!field->is_repeated())
        {
            bool firstHasField = first.GetReflection()->HasField(first, field);
            bool secondHasField = second.GetReflection()->HasField(second, field);
            if (!firstHasField && !secondHasField)
                continue;
            if (firstHasField != secondHasField)
                throw std::runtime_error("one of messages " + first.GetDescriptor()->name() + " has no field " + field->name() + " while another has");

            switch(field->cpp_type())
            {
            case google::protobuf::FieldDescriptor::CPPTYPE_INT32:
                if (first.GetReflection()->GetInt32(first, field) != second.GetReflection()->GetInt32(second, field))
                {
                    throw std::runtime_error(field->name() + " in " + first.GetDescriptor()->name() + " is not equal: " +
                                             std::to_string(first.GetReflection()->GetInt32(first, field)) + " vs " +
                                             std::to_string(second.GetReflection()->GetInt32(second, field)));
                }
                break;
            case google::protobuf::FieldDescriptor::CPPTYPE_INT64:
                if (first.GetReflection()->GetInt64(first, field) != second.GetReflection()->GetInt64(second, field))
                {
                    throw std::runtime_error(field->name() + " in " + first.GetDescriptor()->name() + " is not equal: " +
                                             std::to_string(first.GetReflection()->GetInt64(first, field)) + " vs " +
                                             std::to_string(second.GetReflection()->GetInt64(second, field)));
                }
                break;
            case google::protobuf::FieldDescriptor::CPPTYPE_UINT32:
                if (first.GetReflection()->GetUInt32(first, field) != second.GetReflection()->GetUInt32(second, field))
                {
                    throw std::runtime_error(field->name() + " in " + first.GetDescriptor()->name() + " is not equal: " +
                                             std::to_string(first.GetReflection()->GetUInt32(first, field)) + " vs " +
                                             std::to_string(second.GetReflection()->GetUInt32(second, field)));
                }
                break;
            case google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
                if (first.GetReflection()->GetUInt64(first, field) != second.GetReflection()->GetUInt64(second, field))
                {
                    throw std::runtime_error(field->name() + " in " + first.GetDescriptor()->name() + " is not equal: " +
                                             std::to_string(first.GetReflection()->GetUInt64(first, field)) + " vs " +
                                             std::to_string(second.GetReflection()->GetUInt64(second, field)));
                }
                break;
            case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
                if (first.GetReflection()->GetDouble(first, field) != second.GetReflection()->GetDouble(second, field))
                {
                    throw std::runtime_error(field->name() + " in " + first.GetDescriptor()->name() + " is not equal: " +
                                             std::to_string(first.GetReflection()->GetDouble(first, field)) + " vs " +
                                             std::to_string(second.GetReflection()->GetDouble(second, field)));
                }
                break;
            case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT:
                if (first.GetReflection()->GetFloat(first, field) != second.GetReflection()->GetFloat(second, field))
                {
                    throw std::runtime_error(field->name() + " in " + first.GetDescriptor()->name() + " is not equal: " +
                                             std::to_string(first.GetReflection()->GetFloat(first, field)) + " vs " +
                                             std::to_string(second.GetReflection()->GetFloat(second, field)));
                }
                break;
            case google::protobuf::FieldDescriptor::CPPTYPE_BOOL:
                if (first.GetReflection()->GetBool(first, field) != second.GetReflection()->GetBool(second, field))
                {
                    throw std::runtime_error(field->name() + " in " + first.GetDescriptor()->name() + " is not equal: " +
                                             std::to_string(first.GetReflection()->GetBool(first, field)) + " vs " +
                                             std::to_string(second.GetReflection()->GetBool(second, field)));
                }
                break;
            case google::protobuf::FieldDescriptor::CPPTYPE_ENUM:
                if (first.GetReflection()->GetEnumValue(first, field) != second.GetReflection()->GetEnumValue(second, field))
                {
                    throw std::runtime_error(field->name() + " in " + first.GetDescriptor()->name() + " is not equal: " +
                                             std::to_string(first.GetReflection()->GetEnumValue(first, field)) + " vs " +
                                             std::to_string(second.GetReflection()->GetEnumValue(second, field)));
                }
                break;
            case google::protobuf::FieldDescriptor::CPPTYPE_STRING:
                if (first.GetReflection()->GetString(first, field) != second.GetReflection()->GetString(second, field))
                {
                    throw std::runtime_error(field->name() + " in " + first.GetDescriptor()->name() + " is not equal: " +
                                             first.GetReflection()->GetString(first, field) + " vs " +
                                             second.GetReflection()->GetString(second, field));
                }
                break;
            case google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE:
                EqualMessages(first.GetReflection()->GetMessage(first, field), second.GetReflection()->GetMessage(second, field));
                break;
            }
        }
        else
        {
            int size = first.GetReflection()->FieldSize(first, field);
            if (size != second.GetReflection()->FieldSize(second, field))
                throw std::runtime_error("not equal size of repeated field " + field->name() + " in " + first.GetDescriptor()->name());

            for (int j = 0; j < size; ++j)
            {
                switch(field->cpp_type())
                {
                case google::protobuf::FieldDescriptor::CPPTYPE_INT32:
                    if (first.GetReflection()->GetRepeatedInt32(first, field, j) != second.GetReflection()->GetRepeatedInt32(second, field, j))
                    {
                        throw std::runtime_error(field->name() + " [" + std::to_string(j) + "] in " + first.GetDescriptor()->name() + " is not equal: " +
                                                 std::to_string(first.GetReflection()->GetRepeatedInt32(first, field, j)) + " vs " +
                                                 std::to_string(second.GetReflection()->GetRepeatedInt32(second, field, j)));
                    }
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_INT64:
                    if (first.GetReflection()->GetRepeatedInt64(first, field, j) != second.GetReflection()->GetRepeatedInt64(second, field, j))
                    {
                        throw std::runtime_error(field->name() + " [" + std::to_string(j) + "] in " + first.GetDescriptor()->name() + " is not equal: " +
                                                 std::to_string(first.GetReflection()->GetRepeatedInt64(first, field, j)) + " vs " +
                                                 std::to_string(second.GetReflection()->GetRepeatedInt64(second, field, j)));
                    }
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_UINT32:
                    if (first.GetReflection()->GetRepeatedUInt32(first, field, j) == second.GetReflection()->GetRepeatedUInt32(second, field, j))
                    {
                        throw std::runtime_error(field->name() + " [" + std::to_string(j) + "] in " + first.GetDescriptor()->name() + " is not equal: " +
                                                 std::to_string(first.GetReflection()->GetRepeatedUInt32(first, field, j)) + " vs " +
                                                 std::to_string(second.GetReflection()->GetRepeatedUInt32(second, field, j)));
                    }
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
                    if (first.GetReflection()->GetRepeatedUInt64(first, field, j) != second.GetReflection()->GetRepeatedUInt64(second, field, j))
                    {
                        throw std::runtime_error(field->name() + " [" + std::to_string(j) + "] in " + first.GetDescriptor()->name() + " is not equal: " +
                                                 std::to_string(first.GetReflection()->GetRepeatedUInt64(first, field, j)) + " vs " +
                                                 std::to_string(second.GetReflection()->GetRepeatedUInt64(second, field, j)));
                    }
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
                    if (first.GetReflection()->GetRepeatedDouble(first, field, j) != second.GetReflection()->GetRepeatedDouble(second, field, j))
                    {
                        throw std::runtime_error(field->name() + " [" + std::to_string(j) + "] in " + first.GetDescriptor()->name() + " is not equal: " +
                                                 std::to_string(first.GetReflection()->GetRepeatedDouble(first, field, j)) + " vs " +
                                                 std::to_string(second.GetReflection()->GetRepeatedDouble(second, field, j)));
                    }
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT:
                    if (first.GetReflection()->GetRepeatedFloat(first, field, j) != second.GetReflection()->GetRepeatedFloat(second, field, j))
                    {
                        throw std::runtime_error(field->name() + " [" + std::to_string(j) + "] in " + first.GetDescriptor()->name() + " is not equal: " +
                                                 std::to_string(first.GetReflection()->GetRepeatedFloat(first, field, j)) + " vs " +
                                                 std::to_string(second.GetReflection()->GetRepeatedFloat(second, field, j)));
                    }
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_BOOL:
                    if (first.GetReflection()->GetRepeatedBool(first, field, j) != second.GetReflection()->GetRepeatedBool(second, field, j))
                    {
                        throw std::runtime_error(field->name() + " [" + std::to_string(j) + "] in " + first.GetDescriptor()->name() + " is not equal: " +
                                                 std::to_string(first.GetReflection()->GetRepeatedBool(first, field, j)) + " vs " +
                                                 std::to_string(second.GetReflection()->GetRepeatedBool(second, field, j)));
                    }
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_ENUM:
                    if (first.GetReflection()->GetRepeatedEnumValue(first, field, j) != second.GetReflection()->GetRepeatedEnumValue(second, field, j))
                    {
                        throw std::runtime_error(field->name() + " [" + std::to_string(j) + "] in " + first.GetDescriptor()->name() + " is not equal: " +
                                                 std::to_string(first.GetReflection()->GetRepeatedEnumValue(first, field, j)) + " vs " +
                                                 std::to_string(second.GetReflection()->GetRepeatedEnumValue(second, field, j)));
                    }
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_STRING:
                    if (first.GetReflection()->GetRepeatedString(first, field, j) != second.GetReflection()->GetRepeatedString(second, field, j))
                    {
                        throw std::runtime_error(field->name() + " [" + std::to_string(j) + "] in " + first.GetDescriptor()->name() + " is not equal: " +
                                                 first.GetReflection()->GetRepeatedString(first, field, j) + " vs " +
                                                 second.GetReflection()->GetRepeatedString(second, field, j));
                    }
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE:
                    EqualMessages(first.GetReflection()->GetRepeatedMessage(first, field, j), second.GetReflection()->GetRepeatedMessage(second, field, j));
                    break;
                }
            }
        }
    }
}

TEST_CASE("Complex test", "[smoketest]") {
    Database db;

    srand(0);

    /*
     *  message ComplexMessage {
     *      message NestedMessage {
     *          string name = 1;
     *          repeated int32 value = 2;
     *      }
     *      repeated NestedMessage msg = 1;
     *
     *      message MapMessage {
     *          int64 value = 1;
     *          repeated string str = 2;
     *      }
     *      map<string, MapMessage> messageMap = 2;
     *
     *      repeated int32 values = 3;
     *
     *      string str = 4;
     *      int32 numValue = 5;
     *  }
     */

    REQUIRE_NOTHROW(db.createTable<ComplexMessage>());

    std::vector<ComplexMessage> messages;
    for (int i = 0; i < 3; ++i)
    {
        ComplexMessage msg;
        for (int j = 0; j < (rand() % 5) + 2; ++j)
        {
            ComplexMessage::NestedMessage nested;
            nested.set_name(generate_random_string((rand() % 20) + 1));
            for (int k = 0; k < (rand() % 10) + 1; ++k)
                nested.add_value(rand());
            msg.mutable_msg()->Add(std::move(nested));
        }

        for (int j = 0; j < (rand() % 10) + 10; ++j)
            msg.mutable_values()->Add(rand());

        for (int j = 0; j < (rand() % 5) + 2; ++j)
        {
            ComplexMessage::MapMessage mapMsg;
            mapMsg.set_value(rand());
            for (int k = 0; k < (rand() % 10) + 1; ++k)
                mapMsg.mutable_str()->Add(generate_random_string((rand() % 20) + 1));
            msg.mutable_messagemap()->emplace(generate_random_string((rand() % 20) + 1), std::move(mapMsg));
        }

        msg.set_str(generate_random_string((rand() % 20) + 1));
        msg.set_numvalue(rand());
        REQUIRE_NOTHROW(db.writeMessage(msg));
        messages.emplace_back(std::move(msg));
    }

    std::vector<ComplexMessage> receivedMessages;
    REQUIRE_NOTHROW(receivedMessages = db.getAllMessages<ComplexMessage>());
    REQUIRE(receivedMessages.size() == messages.size());
    for (int i = 0; i < messages.size(); ++i)
    {
        REQUIRE(receivedMessages[i].msg_size() == messages[i].msg_size());
        REQUIRE(receivedMessages[i].messagemap_size() == messages[i].messagemap_size());
        REQUIRE(receivedMessages[i].values_size() == messages[i].values_size());
        REQUIRE(receivedMessages[i].str() == messages[i].str());
        REQUIRE(receivedMessages[i].numvalue() == messages[i].numvalue());

        REQUIRE_NOTHROW(EqualMessages(receivedMessages[i], messages[i]));
    }
}

TEST_CASE("Keys test", "[smoketest]") {
    Database db;

    srand(0);

    /*
        message TestKeyMessage {
            int32 index = 1 [(ProtoDatabase.Proto.objectKeyField) = true];

            repeated int64 numValues = 3;
            string data = 4;
        }
     */

    REQUIRE_NOTHROW(db.createTable<TestKeyMessage>());

    std::vector<TestKeyMessage> msgList;
    for (int i = 0; i < 5; ++i)
    {
        TestKeyMessage msg;
        msg.set_index(10 * i);

        for (int j = 0; j < (rand() % 10) + 5; ++j)
        {
            msg.mutable_numvalues()->Add(rand());
        }

        msg.set_data(generate_random_string(10));
        REQUIRE_NOTHROW(db.writeMessage(msg));
        msgList.emplace_back(std::move(msg));
    }

    std::random_device rd;
    std::mt19937 g(rd());

    std::shuffle(msgList.begin(), msgList.end(), g);
    for (int i = 0; i < msgList.size(); ++i)
    {
        auto msg = db.findMessage<TestKeyMessage, int>(TestKeyMessage::GetDescriptor()->FindFieldByNumber(TestKeyMessage::kIndexFieldNumber), msgList[i].index());
        REQUIRE(msg.has_value());
        REQUIRE_NOTHROW(EqualMessages(*msg, msgList[i]));
    }

    std::optional<TestKeyMessage> msg;
    REQUIRE_NOTHROW(msg = db.findMessage<TestKeyMessage, int>(TestKeyMessage::GetDescriptor()->FindFieldByNumber(TestKeyMessage::kIndexFieldNumber), 15));
    REQUIRE(!msg);
}

TEST_CASE("Complex key test", "[smoketest]") {
    Database db;

    srand(0);

    /*
        message ComplexKeyTestMessage {
            message Position {
                int32 x = 1;
                int32 y = 2;
            }

            Position pos = 1 [(ProtoDatabase.Proto.objectKeyField) = true];

            string data = 2;
            repeated int64 numValues = 3;

            enum TestEnum {
                val0 = 0;
                val1 = 1;
                val2 = 2;
                val3 = 3;
            }
            TestEnum enumValue = 4;
        }
     */

    REQUIRE_NOTHROW(db.createTable<ComplexKeyTestMessage>());

    std::vector<ComplexKeyTestMessage> msgList;
    for (int i = 0; i < 5; ++i)
    {
        ComplexKeyTestMessage msg;

        msg.mutable_pos()->set_x(rand());
        msg.mutable_pos()->set_y(rand());

        msg.set_data(generate_random_string(10));
        for (int j = 0; j < (rand() % 10) + 5; ++j)
        {
            msg.mutable_numvalues()->Add(rand());
        }

        msg.set_enumvalue(static_cast<ComplexKeyTestMessage::TestEnum>(rand() % ComplexKeyTestMessage_TestEnum_TestEnum_ARRAYSIZE));

        REQUIRE_NOTHROW(db.writeMessage(msg));
        msgList.emplace_back(std::move(msg));
    }

    std::random_device rd;
    std::mt19937 g(rd());

    std::shuffle(msgList.begin(), msgList.end(), g);
    for (int i = 0; i < msgList.size(); ++i)
    {
        auto msg = db.findMessage<ComplexKeyTestMessage>(ComplexKeyTestMessage::GetDescriptor()->FindFieldByNumber(ComplexKeyTestMessage::kPosFieldNumber), msgList[i].pos());
        REQUIRE(msg.has_value());
        REQUIRE_NOTHROW(EqualMessages(*msg, msgList[i]));
    }

    ComplexKeyTestMessage::Position fakePos;
    fakePos.set_x(4);
    fakePos.set_y(8);

    std::optional<ComplexKeyTestMessage> msg;
    REQUIRE_NOTHROW(msg = db.findMessage<ComplexKeyTestMessage>(ComplexKeyTestMessage::GetDescriptor()->FindFieldByNumber(ComplexKeyTestMessage::kPosFieldNumber), fakePos));
    REQUIRE(!msg);
}

TEST_CASE("Key duplication test", "[smoketest]") {
    Database db;

    srand(0);

    /*
        message TestKeyMessage {
            int32 index = 1 [(ProtoDatabase.Proto.objectKeyField) = true];

            repeated int64 numValues = 3;
            string data = 4;
        }
     */

    REQUIRE_NOTHROW(db.createTable<TestKeyMessage>());

    TestKeyMessage msg;
    msg.set_index(1);

    for (int j = 0; j < (rand() % 10) + 5; ++j)
    {
        msg.mutable_numvalues()->Add(rand());
    }

    msg.set_data(generate_random_string(10));
    REQUIRE_NOTHROW(db.insertMessage(msg));

    TestKeyMessage msg2;
    msg2.set_index(1);

    for (int j = 0; j < (rand() % 10) + 5; ++j)
    {
        msg2.mutable_numvalues()->Add(rand());
    }

    msg2.set_data(generate_random_string(10));
    REQUIRE_THROWS(db.insertMessage(msg2));
}

TEST_CASE("Complex key duplication test", "[smoketest]") {
    Database db;

    srand(0);

    /*
        message ComplexKeyTestMessage {
            message Position {
                int32 x = 1;
                int32 y = 2;
            }

            Position pos = 1 [(ProtoDatabase.Proto.objectKeyField) = true];

            string data = 2;
            repeated int64 numValues = 3;

            enum TestEnum {
                val0 = 0;
                val1 = 1;
                val2 = 2;
                val3 = 3;
            }
            TestEnum enumValue = 4;
        }
     */

    REQUIRE_NOTHROW(db.createTable<ComplexKeyTestMessage>());

    ComplexKeyTestMessage::Position pos;
    pos.set_x(4);
    pos.set_y(8);

    ComplexKeyTestMessage msg;
    *msg.mutable_pos() = pos;

    for (int j = 0; j < (rand() % 10) + 5; ++j)
    {
        msg.mutable_numvalues()->Add(rand());
    }

    msg.set_data(generate_random_string(10));
    REQUIRE_NOTHROW(db.insertMessage(msg));

    ComplexKeyTestMessage msg2;
    *msg2.mutable_pos() = pos;

    for (int j = 0; j < (rand() % 10) + 5; ++j)
    {
        msg2.mutable_numvalues()->Add(rand());
    }

    msg2.set_data(generate_random_string(10));
    REQUIRE_THROWS(db.insertMessage(msg2));
}

TEST_CASE("Deletion test", "[smoketest]") {
    Database db;

    REQUIRE_NOTHROW(db.createTable<ComplexKeyTestMessage>());

    ComplexKeyTestMessage::Position pos1;
    pos1.set_x(4);
    pos1.set_y(8);

    std::vector<ComplexKeyTestMessage> objects;
    {
        ComplexKeyTestMessage msg;
        *msg.mutable_pos() = pos1;

        for (int j = 0; j < (rand() % 10) + 5; ++j)
        {
            msg.mutable_numvalues()->Add(rand());
        }

        msg.set_data(generate_random_string(10));
        REQUIRE_NOTHROW(db.insertMessage(msg));
        objects.emplace_back(std::move(msg));
    }

    ComplexKeyTestMessage::Position pos2;
    pos2.set_x(15);
    pos2.set_y(16);

    {
        ComplexKeyTestMessage msg;
        *msg.mutable_pos() = pos2;

        for (int j = 0; j < (rand() % 10) + 5; ++j)
        {
            msg.mutable_numvalues()->Add(rand());
        }

        msg.set_data(generate_random_string(10));
        REQUIRE_NOTHROW(db.insertMessage(msg));
        objects.emplace_back(std::move(msg));
    }

    {
        std::vector<ComplexKeyTestMessage> res;
        REQUIRE_NOTHROW(res = db.getAllMessages<ComplexKeyTestMessage>());
        REQUIRE(res.size() == 2);
    }

    {
        std::vector<ComplexKeyTestMessage::Position> posList;
        REQUIRE_NOTHROW(posList = db.getAllMessages<ComplexKeyTestMessage::Position>());
        REQUIRE(posList.size() == 2);
    }

    REQUIRE_NOTHROW(db.deleteMessage<ComplexKeyTestMessage>(ComplexKeyTestMessage::GetDescriptor()->FindFieldByNumber(ComplexKeyTestMessage::kPosFieldNumber), pos1));

    {
        std::vector<ComplexKeyTestMessage> res;
        REQUIRE_NOTHROW(res = db.getAllMessages<ComplexKeyTestMessage>());
        REQUIRE(res.size() == 1);
        REQUIRE_NOTHROW(EqualMessages(res[0], objects[1]));
    }

    {
        std::vector<ComplexKeyTestMessage::Position> posList;
        REQUIRE_NOTHROW(posList = db.getAllMessages<ComplexKeyTestMessage::Position>());
        REQUIRE(posList.size() == 1);
        REQUIRE_NOTHROW(EqualMessages(posList[0], pos2));
    }

    REQUIRE_NOTHROW(db.deleteMessage<ComplexKeyTestMessage>(ComplexKeyTestMessage::GetDescriptor()->FindFieldByNumber(ComplexKeyTestMessage::kPosFieldNumber), pos1));
    REQUIRE_NOTHROW(db.deleteMessage<ComplexKeyTestMessage>(ComplexKeyTestMessage::GetDescriptor()->FindFieldByNumber(ComplexKeyTestMessage::kPosFieldNumber), pos2));

    {
        std::vector<ComplexKeyTestMessage> res;
        REQUIRE_NOTHROW(res = db.getAllMessages<ComplexKeyTestMessage>());
        REQUIRE(res.empty());
    }

    {
        std::vector<ComplexKeyTestMessage::Position> posList;
        REQUIRE_NOTHROW(posList = db.getAllMessages<ComplexKeyTestMessage::Position>());
        REQUIRE(posList.empty());
    }
}

TEST_CASE("Data selection test", "[smoketest]") {
    Database db;

    srand(0);

    /*
        message StringKeyMessage {
            string name = 1 [(ProtoDatabase.Proto.objectKeyField) = true];
            uint64 number = 2;
            float floatNumber = 3;
        }
     */

    REQUIRE_NOTHROW(db.createTable<StringKeyMessage>());

    std::vector<float> data;
    data.reserve(100);
    std::vector<std::string> names;
    names.reserve(100);
    for (size_t i = 0; i < 100; ++i)
    {
        data.push_back(rand());

        StringKeyMessage msg;
        msg.set_name(generate_random_string(20));
        msg.set_floatnumber(data[i]);

        names.push_back(msg.name());

        REQUIRE_NOTHROW(db.writeMessage(msg));
    }

    {
        std::vector<float> res;
        REQUIRE_NOTHROW(res = db.getValue<float, StringKeyMessage>(StringKeyMessage::GetDescriptor()->FindFieldByNumber(StringKeyMessage::kFloatNumberFieldNumber)));
        REQUIRE(res == data);
    }

    {
        std::vector<std::string> res;
        REQUIRE_NOTHROW(res = db.getValue<std::string, StringKeyMessage>(StringKeyMessage::GetDescriptor()->FindFieldByNumber(StringKeyMessage::kNameFieldNumber)));
        REQUIRE(res == names);
    }
}
