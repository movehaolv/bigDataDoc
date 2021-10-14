package com.atguigu.weibo.test;

import com.atguigu.weibo.contants.Constants;
import com.atguigu.weibo.dao.HBaseDao;
import com.atguigu.weibo.utils.HBaseUtil;

import java.io.IOException;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 11:00 2021/4/18
 */


public class TestWeibo {
    public static void init() {
        try{
            HBaseUtil.createNameSpace(Constants.NAMESPACE);

            HBaseUtil.createTable(Constants.CONTENT_TABLE, Constants.CONTENT_TABLE_VERSIONS, Constants.CONTENT_TABLE_CF);
            HBaseUtil.createTable(Constants.RELATION_TABLE, Constants.RELATION_TABLE_VERSIONS,
                    Constants.RELATION_TABLE_CF1, Constants.RELATION_TABLE_CF2);
            HBaseUtil.createTable(Constants.INBOX_TABLE, Constants.INBOX_TABLE_VERSIONS, Constants.INBOX_TABLE_CF);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        init();

        HBaseDao.publishWeiBo("1001", "1001-1111111111111");
        //1002关注1001和1003
        HBaseDao.addAttends("1002", "1001", "1003");
        //获取1002初始化界面
        System.out.println("---------------11111----------------");
        HBaseDao.getInit("1002");
        //1003发布三条微博，同时1001发布两条
        HBaseDao.publishWeiBo("1003", "1003-111111111");
        Thread.sleep(100);
        HBaseDao.publishWeiBo("1001", "1001-2222222");
        HBaseDao.publishWeiBo("1003", "1003-222222222");
        Thread.sleep(100);
        HBaseDao.publishWeiBo("1001", "1001-3333333333333");
        HBaseDao.publishWeiBo("1003", "1003-33333333333");

        //再次获取1002初始化界面
        System.out.println("---------------222222-----------------");
        HBaseDao.getInit("1002");
        //1002取关1003
        HBaseDao.removeAttends("1002", "1003");
        //再次获取1002初始化界面
        System.out.println("---------------3333333-----------------");
        HBaseDao.getInit("1002");
        //1002再次关注1003
        HBaseDao.addAttends("1002", "1003");
        //获取1002初始化界面
        System.out.println("---------------44444-----------------");
        HBaseDao.getInit("1002");
        //获取1001微博详情
        HBaseDao.getWeiBo("1001");
        HBaseDao.getWeiBo("1003");
    }

}
