#include <iostream>
#include <chrono>

class MeasureExecutionTime{
  private:
      const std::chrono::steady_clock::time_point begin;
      const std::string caller;
  public:
      MeasureExecutionTime(const std::string& caller):caller(caller), begin(std::chrono::steady_clock::now()){}
      ~MeasureExecutionTime(){
          const auto duration=std::chrono::steady_clock::now()-begin;
          double value = (double)std::chrono::duration_cast<std::chrono::microseconds>(duration).count()/1000;
          std::cout << caller << " : " << value << " ms" << std::endl;
      }
};
