package com.puming.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author pengcheng
 * @version V1.0
 * @description 线程测试类
 * @date 2019/04/13 11:25
 */
public class ThreadTest {
    public static void main(String[] args) {
//        newFixedThreadPool();
//        newCachedThreadPool();
//        newScheduledThreadPool();
//        newScheduledThreadPool2();
        newSingleThreadExecutor();
    }

    private static void newFixedThreadPool() {
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 100; i++) {
            Runnable sysncRunnable = new Runnable() {
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getName());
                }
            };
            executorService.execute(sysncRunnable);
        }
    }

    private static void newCachedThreadPool() {
        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 0; i < 100; i++) {
            Runnable syncRunnable = new Runnable() {
                @Override
                public void run() {
                    System.out.println((Thread.currentThread().getName()));
                }
            };
            executorService.execute(syncRunnable);
        }
    }

    private static void newScheduledThreadPool() {
//        示例：表示从提交任务开始计时，5000 毫秒后执行
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(5);
        for (int i = 0; i < 20; i++) {
            Runnable syncRunnable = new Runnable() {
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getName());
                }
            };
            executorService.schedule(syncRunnable, 5000, TimeUnit.MILLISECONDS);
        }
    }

    private static void newScheduledThreadPool2() {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(5);
        Runnable syncRunnable = new Runnable() {
            @Override
            public void run() {
                System.out.println(Thread.currentThread().getName());
            }
        };
        executorService.scheduleAtFixedRate(syncRunnable, 5000, 3000, TimeUnit.MILLISECONDS);
    }

    private static void newSingleThreadExecutor() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        for (int i = 0; i < 20; i++) {
            Runnable syncRunnable = new Runnable() {
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getName());
                }
            };
            executorService.execute(syncRunnable);
        }
    }

}
