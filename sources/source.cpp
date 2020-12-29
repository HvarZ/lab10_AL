// Copyright 2020 Vumba798 <alexandrov32649@gmail.com>

#include <control_sum.hpp>
#include <iostream>
#include <thread>
#include <cstdint>
#include <stdexcept>
#include <boost/program_options.hpp>

namespace po = boost::program_options;


int main(int argc, char** argv) {
  string input, logLevel, output;
  uint32_t threadCount = 0;

  // Parsing program options
  po::options_description visible("Options:");
  visible.add_options()
      ("help", "produce help message")
      ("log-level", po::value<string>()->default_value("error"),
       "\"info\"|\"warning\"|\"debug\"")
      ("thread-count", po::value<uint32_t>()->default_value(
          std::thread::hardware_concurrency()), "")
      ("output", po::value<string>()->default_value(
          "path/to/output/storage_db"), "<path/to/output/storage_db>");

  po::options_description hidden("Hidden options");
  hidden.add_options()
      ("input", po::value<string>()->default_value("path/to/input/storage_db"), "");

  po::positional_options_description pos;
  pos.add("input", -1);

  po::options_description all("Allowed options:");
  all.add(visible).add(hidden);

  po::variables_map vm;
  po::store(po::command_line_parser(argc, argv).options(all).positional(pos).run(), vm);
  po::notify(vm);

  // Setting init variables
  if (vm.count("help")) {
    std::cout << "Usage:\n\n\tdbcs [options] <path/to/input/storage_db>\n\n\n"
              << visible << endl;;
    return 1;
  }
  if (vm.count("input")) {
    std::cout << "input was set to"
              << vm["input"].as<string>() << endl;
    input = vm["input"].as<string>();
  }
  if (vm.count("log-level")) {
    std::cout << "log-level was set to "
              << vm["log-level"].as<string>() << endl;
    logLevel = vm["log-level"].as<string>();
  } else {
    std::cout << "log-level was set to default" << endl;
    logLevel = vm["log-level"].as<string>();
    std::cout << logLevel << std::endl;
  }
  if (vm.count("thread-count")) {
    std::cout << "thread-count was set to "
              << vm["thread-count"].as<uint32_t>() << endl;
    threadCount = vm["thread-count"].as<uint32_t>();
  } else {
    std::cout << "thread-count was set to default" << endl;
    threadCount = vm["thread-count"].as<uint32_t>();
  }
  if (vm.count("output")) {
    std::cout << "output was set to "
              << vm["output"].as<string>() << endl;
    output = vm["output"].as<string>();
  } else {
    std::cout << "output was set to default" << endl;
    output = vm["output"].as<string>();
  }

  // Setting boost log
  auto sinkConsole = boost::log::add_console_log(
      std::cout,
      boost::log::keywords::format = "[%TimeStamp%]: %Message%");
  if (logLevel == "info") {
    sinkConsole->set_filter(
        boost::log::trivial::severity >= boost::log::trivial::info);
  } else if (logLevel == "warning") {
    sinkConsole->set_filter(
        boost::log::trivial::severity >= boost::log::trivial::warning);
  } else if (logLevel == "error") {
    sinkConsole->set_filter(
        boost::log::trivial::severity >= boost::log::trivial::error);
  } else {
    throw std::runtime_error("Unknown log-level parameter");
  }
  boost::log::add_common_attributes();

  BOOST_LOG_TRIVIAL(info)
      << "Initial options:"
      << "\n\tTHREADS: " << threadCount
      << "\n\tLOGS: " << logLevel
      << "\n\tINPUT: " << input
      << "\n\tOUTPUT: " << output;

  dbsc::control_sum cs(threadCount, input, output);
  dbsc::start(cs);
  return 0;
}