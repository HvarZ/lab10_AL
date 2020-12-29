// Copyright 2020 Vumba798 <alexandrov32649@gmail.com>

#include <control_sum.hpp>
#include <boost/asio/post.hpp>
#include <picosha2.h>
#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <iostream>
#include <functional>

dbsc::control_sum::control_sum(const uint32_t& amountOfThreads,
                               const string& input,
                               const string& output) :
    _pool(amountOfThreads),
    _input(input), _output(output) {}

void dbsc::_write_db(control_sum& cm) {
  try {
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* db = nullptr;
    rocksdb::WriteBatch batch;
    vector<rocksdb::ColumnFamilyHandle*> handles;
    handles.resize(cm._columnNames.size());

    rocksdb::Status s = db->Open(options, cm._output, &db);
    if (!s.ok()) {
      BOOST_LOG_TRIVIAL(error) << "An error has occured while opening"
                               << " a database to write: "
                               << s.ToString() << endl;
    }

    for (uint32_t i = 0; i < cm._columnNames.size(); ++i) {
      BOOST_LOG_TRIVIAL(info) << "Creating column families...";
      BOOST_LOG_TRIVIAL(info) << "Amount of columns: " << cm._columnNames.size();
      db->CreateColumnFamily(
          rocksdb::ColumnFamilyOptions(), cm._columnNames[i], &handles[i]);
      for (auto it = cm._data[i].begin(); it != cm._data[i].end(); ++it) {
        BOOST_LOG_TRIVIAL(info) << "Preparing to write an element: "
                                << it->first << " : " << it->second;
        batch.Put(handles[i],
                  rocksdb::Slice(it->first),
                  rocksdb::Slice(it->second));
      }
    }
    BOOST_LOG_TRIVIAL(info) << "Writing data...";
    db->Write(rocksdb::WriteOptions(), &batch);

    BOOST_LOG_TRIVIAL(info) << "Closing database...";
    for (auto handle : handles) {
      db->DestroyColumnFamilyHandle(handle);
    }
    delete db;
  } catch (const std::exception& e) {
    BOOST_LOG_TRIVIAL(error)
        << "A terminal error has occured while writing databese: "
        << e.what();
    throw e;
  }
}

void dbsc::_read_db(control_sum& cm) {
    BOOST_LOG_TRIVIAL(warning) << "Reading database, path: " << cm._input;
    rocksdb::Options options;
    options.create_if_missing = false;
    rocksdb::DB* db = nullptr;

    vector<rocksdb::ColumnFamilyDescriptor> columnFamilies;
    vector<rocksdb::ColumnFamilyHandle*> handles;

    rocksdb::DB::ListColumnFamilies(rocksdb::DBOptions(), cm._input, &cm._columnNames);
    cm._data.resize(cm._columnNames.size());
    BOOST_LOG_TRIVIAL(info) << "Amount of columns: " << cm._columnNames.size();
    BOOST_LOG_TRIVIAL(info) << "Reading list of columns...";

    for (auto name : cm._columnNames) {
      BOOST_LOG_TRIVIAL(info) << "Name of column: " << name;
      columnFamilies.push_back(rocksdb::ColumnFamilyDescriptor(
          name, rocksdb::ColumnFamilyOptions()));
    }
    auto s = db->OpenForReadOnly(rocksdb::Options(), cm._input,
                                 columnFamilies, &handles, &db, false);
    if (!s.ok()) {
      BOOST_LOG_TRIVIAL(error) << "An error has occured while openning a database: "
                               << s.ToString();
      return;
    }
    for (uint32_t i = 0; i < cm._columnNames.size(); ++i) {
      rocksdb::Iterator* iterator = db->NewIterator(
          rocksdb::ReadOptions(), handles[i]);
      BOOST_LOG_TRIVIAL(info) << "Reading column family \""
                              << cm._columnNames[i] << "\"...";
      for (iterator->Seek("k"); iterator->Valid(); iterator->Next()) {
        auto key = iterator->key().data();
        auto value = iterator->value().data();

        BOOST_LOG_TRIVIAL(info) << "Key: " << key
                                << "\tValue: " << value;

        pair_key_value key_value(key, value);
        boost::asio::post(cm._pool, std::bind(&dbsc::_calculate_hash,cm, i, key_value));
      }
    }
    BOOST_LOG_TRIVIAL(warning) << "Successfully read";
    for (auto handle : handles) {
      db->DestroyColumnFamilyHandle(handle);
    }

    delete db;
    cm._pool.join();
}

void dbsc::start(control_sum& cm) {
  BOOST_LOG_TRIVIAL(error) << "Starting control_sum...";
  dbsc::_read_db(cm);
  BOOST_LOG_TRIVIAL(warning) << "Writing database...";
  dbsc::_write_db(cm);
  BOOST_LOG_TRIVIAL(error) << "control_sum executed successfully!";
}

void dbsc::_calculate_hash(control_sum& cm, const uint32_t i,
                                  const pair_key_value key_value) {
  string hash_hex;
  picosha2::hash256_hex_string(key_value.value, hash_hex);
  cm._mutex.lock();
  cm._data[i][key_value.key] = hash_hex;
  cm._mutex.unlock();
  BOOST_LOG_TRIVIAL(info)
      << "For key " << key_value.key
      << " hash was calculated successfully: "
      << key_value.value  << " -> " << cm._data[i][key_value.key];
}

