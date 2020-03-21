package com.crazymakercircle.zk.distributedLock;

import com.crazymakercircle.zk.ZkClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.Watcher;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ZkLock implements Lock {
    //ZkLock的节点链接
    private static final String ZK_PATH = "/test/lock";
    private static final String LOCK_PREFIX = ZK_PATH + "/";
    private static final long WAIT_TIME = 1000;
    //Zk客户端
    CuratorFramework client;

    private String lockedShortPath = null;
    private String lockedPath = null;
    private String priorPath = null;
    final AtomicInteger lockCount = new AtomicInteger(0);
    private Thread thread;

    public ZkLock() {
        ZkClient.instance.init();
        if (!ZkClient.instance.isNodeExist(ZK_PATH)) {
            ZkClient.instance.createNode(ZK_PATH, null);
        }
        client = ZkClient.instance.getClient();
    }

    @Override
    public boolean lock() {
        //可重入，确保同一个进程可以重复加锁
        synchronized (this) {
            if (lockCount.get() == 0) {
                thread = Thread.currentThread();
                lockCount.incrementAndGet();
            } else {
                if (!thread.equals(Thread.currentThread())) {
                    return false;
                }
                lockCount.incrementAndGet();
                return true;
            }
        }

        try {
            boolean locked = tryLock();
            if (locked) {
                return true;
            }
            while (!locked) {
                await();
                //获取等待的子节点列表
                List<String> waiters = getWaiters();
                if (checkLocked(waiters)) {
                    locked = true;
                }
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            unlock();
        }
        return false;
    }


    @Override
    public boolean unlock() {
        if (!thread.equals(Thread.currentThread())) {
            return false;
        }

        int newLockCount = lockCount.decrementAndGet();
        if (newLockCount < 0) {
            throw new IllegalMonitorStateException("Lock count has gone negative for lock: " + lockedPath);
        }

        if (newLockCount != 0) {
            return true;
        }
        try {
            if (ZkClient.instance.isNodeExist(lockedPath)) {
                client.delete().forPath(lockedPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    /**
     * 等待
     * 监听前一个节点的删除事件
     *
     * @throws Exception 可能会有Zk异常、网络异常
     */
    private void await() throws Exception {
        if (null == priorPath) {
            throw new Exception("prior_path error");
        }

        final CountDownLatch latch = new CountDownLatch(1);

        //监听方式一：Watcher一次性订阅
        //订阅比自己次小顺序节点的删除事件
        Watcher watcher = watchedEvent -> {
            log.info("监听到的变化 watchedEvent = " + watchedEvent);
            log.info("[WatchedEvent]节点删除");
            latch.countDown();
        };

        client.getData().usingWatcher(watcher).forPath(priorPath);
/*
        //监听方式二：TreeCache订阅
        //订阅比自己次小顺序节点的删除事件
        TreeCache treeCache = new TreeCache(client, prior_path);
        TreeCacheListener l = new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client,
                                   TreeCacheEvent event) throws Exception {
                ChildData data = event.getData();
                if (data != null) {
                    switch (event.getType()) {
                        case NODE_REMOVED:
                            log.debug("[TreeCache]节点删除, path={}, data={}",
                                    data.getPath(), data.getData());

                            latch.countDown();
                            break;
                        default:
                            break;
                    }
                }
            }
        };

        treeCache.getListenable().addListener(l);
        treeCache.start();*/
        latch.await(WAIT_TIME, TimeUnit.SECONDS);
    }

    private boolean tryLock() throws Exception {
        //创建临时ZNode
        List<String> waiters = getWaiters();
        lockedPath = ZkClient.instance
                .createEphemeralSeqNode(LOCK_PREFIX);
        if (null == lockedPath) {
            throw new Exception("zk error");
        }
        lockedShortPath = getShorPath(lockedPath);

        //获取等待的子节点列表，判断自己是否第一个
        if (checkLocked(waiters)) {
            return true;
        }

        //判断自己排第几个
        int index = Collections.binarySearch(waiters, lockedShortPath);
        if (index < 0) { //网络抖动，获取到的子节点列表里可能已经没有自己了
            throw new Exception("节点没有找到: " + lockedShortPath);
        }

        //如果自己没有获得锁，则要监听前一个节点
        priorPath = ZK_PATH + "/" + waiters.get(index - 1);

        return false;
    }

    private String getShorPath(String locked_path) {

        int index = locked_path.lastIndexOf(ZK_PATH + "/");
        if (index >= 0) {
            index += ZK_PATH.length() + 1;
            return index <= locked_path.length() ? locked_path.substring(index) : "";
        }
        return null;
    }

    private boolean checkLocked(List<String> waiters) {
        //节点按照编号，升序排列
        Collections.sort(waiters);

        //如果是第一个，代表自己已经获得了锁
        if (lockedShortPath.equals(waiters.get(0))) {
            log.info("成功的获取分布式锁,节点为{}", lockedShortPath);
            return true;
        }
        return false;
    }

    /**
     * 从zookeeper中拿到所有等待节点
     */
    protected List<String> getWaiters() {
        List<String> children = null;
        try {
            children = client.getChildren().forPath(ZK_PATH);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return children;
    }
}
