package com.crazymakercircle.zk.distributedLock;

import com.crazymakercircle.cocurrent.FutureTaskScheduler;
import com.crazymakercircle.zk.ZkClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.junit.Test;

@Slf4j
public class ZkLockTester {
    int count = 0;

    @Test
    public void testLock() throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            FutureTaskScheduler.add(() -> {
                ZkLock lock = new ZkLock();
                lock.lock();

                for (int j = 0; j < 10; j++) {

                    count++;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                log.info("count = " + count);
                lock.unlock();

            });
        }

        Thread.sleep(Integer.MAX_VALUE);
    }


    @Test
    public void testZkMutex() throws InterruptedException {
        CuratorFramework client = ZkClient.instance.getClient();
        //创建互斥锁
        final InterProcessMutex zkMutex = new InterProcessMutex(client, "/mutex");
        //每条线程执行10次累加
        for (int i = 0; i < 10; i++) {
            FutureTaskScheduler.add(() -> {
                try {
                    //获得互斥锁
                    zkMutex.acquire();
                    for (int j = 0; j < 10; j++) {
                        //公共资源变量累加
                        count++;
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    log.info("count = " + count);
                    //释放互斥锁
                    zkMutex.release();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        Thread.sleep(Integer.MAX_VALUE);
    }


}
