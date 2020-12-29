// Copyright 2020 Vumba798 <alexandrov32649@gmail.com>

#ifndef LAB_10_CONTROL_SUM_HPP
#define LAB_10_CONTROL_SUM_HPP

#include <boost/asio/thread_pool.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup.hpp>
#include <rocksdb/c.h>
#include <atomic>
#include <condition_variable>
#include <unordered_map>

using std::string;
using std::vector;
using boost::asio::thread_pool;
using std::unordered_map;
using std::mutex;
using std::endl;

namespace dbsc {



struct pair_key_value {
  string key;
  string value;
  pair_key_value(const std::string& k, const std::string& v)
      : key(k), value(v) {}
};

struct control_sum {
  thread_pool _pool;
  vector<unordered_map<string, string>> _data;
  vector<string> _columnNames;
  string _input;
  string _output;
  mutex _mutex;

  control_sum(const uint32_t& amountOfThreads, const string& input,
              const string& output);
};

  void _calculate_hash(control_sum& cm, const uint32_t i, const pair_key_value);

  void _read_db(control_sum& cm);

  void _write_db(control_sum& cm);

  void write_test_db(control_sum& cm);

  void start(control_sum& cm);

}


#endif  // LAB_10_CONTROL_SUM_HPP
