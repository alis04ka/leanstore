#include "common/typedefs.h"
#include <chrono>

namespace leanstore::storage::vector {

class Timer {
public:
  Timer()
      : start_time(), end_time() {}

  void start() {
    start_time = std::chrono::high_resolution_clock::now();
  }

  void end() {
    end_time = std::chrono::high_resolution_clock::now();
  }

  i64 getElapsed() const {
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    return elapsed.count();
  }

private:
  std::chrono::high_resolution_clock::time_point start_time;
  std::chrono::high_resolution_clock::time_point end_time;
};

#define TIMER_START(timer) \
    Timer timer;           \
    timer.start();

#define TIMER_END(timer, accumulator) \
    timer.end();                      \
    accumulator += timer.getElapsed();

#ifdef TIME_INDEX
#define START_TIMER(timer) TIMER_START(timer)
#define END_TIMER(timer, accumulator) TIMER_END(timer, accumulator)
#else
#define START_TIMER(timer)
#define END_TIMER(timer, accumulator)
#endif

}