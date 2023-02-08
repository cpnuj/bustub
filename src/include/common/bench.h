#pragma once

#include <chrono>
#include <iostream>
#include <string>

template <typename F>
auto BENCHMARK(F &&func) {
  auto start = std::chrono::system_clock::now();
  func();
  auto end = std::chrono::system_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  std::cout << "Bench result: " << elapsed.count() << " milliseconds" << std::endl;
}