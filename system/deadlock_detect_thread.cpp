#include "deadlock_detect_thread.h"
#include "manager.h"
RC DeadLockDetectThread::run() {
    tsetup();
    int i = 0;
    while (!simulation->is_done()) {
        i = (i + 1) % 2;
        if (i == 0) {
            glob_manager.lock_manager.deathLockDetection(get_thd_id());
        } else {
            glob_manager.calculateBlockTime(get_thd_id());
        }
        
        
        usleep(sleep_microsecond);
    }
    fflush(stdout);
	return FINISH;
}
void DeadLockDetectThread::setup() {
    //Microsecond，等于1000微秒为1毫秒
    sleep_microsecond = 300000;

}