#include "deadlock_detect_thread.h"
#include "manager.h"
RC DeadLockDetectThread::run() {
    tsetup();
    int i = 0;
    while (!simulation->is_done()) {
        i++;
        if (i % 2 == 0) {
            glob_manager.lock_manager.deathLockDetection(get_thd_id());
        } else {
            glob_manager.migration_stat.calculateBlockTime(get_thd_id());
        }
        if (i % 20 == 0) {
            i = 0;
            glob_manager.migration_stat.caculateIntervalThroughput();
        }
        
        usleep(sleep_microsecond);
        
    }
    fflush(stdout);
	return FINISH;
}
void DeadLockDetectThread::setup() {
    //Microsecond，等于1000微秒为1毫秒
    sleep_microsecond = 50000;

}