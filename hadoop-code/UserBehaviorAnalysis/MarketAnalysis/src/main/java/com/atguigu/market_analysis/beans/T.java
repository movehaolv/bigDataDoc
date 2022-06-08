package com.atguigu.market_analysis.beans;

import redis.clients.jedis.Jedis;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 19:16 2021/11/24
 */


public class T {

    public static void main(String[] args) {

        Jedis jedis = new Jedis("localhost", 6379);
        Long flag = jedis.setnx("a", "b");
        System.out.println(flag);

    }
}
