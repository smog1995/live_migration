#include "global.h"
#include "thread.h"
class DeadLockDetectThread: public Thread {
public:
//   DeadLockDetectThread() {}
  RC run();
  void setup();
  uint32_t sleep_microsecond;
};