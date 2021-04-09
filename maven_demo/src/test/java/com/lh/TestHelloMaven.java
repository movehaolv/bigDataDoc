package com.lh;

import org.junit.Assert;
import org.junit.Test;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 9:46 2021/4/9
 */


public class TestHelloMaven {
    @Test
    public void testAdd(){
        System.out.println("+++maven junit testAdd()+++");
        HelloMaven helloMaven = new HelloMaven();
        int add = helloMaven.add(10, 20);
        Assert.assertEquals(30, add);
    }

    @Test
    public void testAdd2(){
        System.out.println("+++maven junit testAdd2()+++");
        HelloMaven helloMaven = new HelloMaven();
        int add = helloMaven.add(10, 20);
        Assert.assertEquals(30, add);
    }

}


