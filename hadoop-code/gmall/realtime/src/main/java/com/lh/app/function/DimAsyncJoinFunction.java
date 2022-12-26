package com.lh.app.function;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 16:49 2022/10/25
 */


public interface DimAsyncJoinFunction<T> {
    String getKey(T input);

    void  join(T input, JSONObject jsonObject) throws ParseException;
}
