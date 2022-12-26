package com.lh.utils;

import org.apache.flink.api.common.time.Time;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 10:42 2022/10/28
 */


public class ThreadPoolUtil {

    static ThreadPoolExecutor threadPoolExecutor;

    public static ThreadPoolExecutor getTreadPoolExecutor(){
        if(threadPoolExecutor==null){
            synchronized (ThreadPoolUtil.class){
                if(threadPoolExecutor==null){
                    threadPoolExecutor = new ThreadPoolExecutor(2, 16, 1L, TimeUnit.MINUTES,
                            new LinkedBlockingDeque<>());
                }
            }
        }
        return threadPoolExecutor;
    }

}
