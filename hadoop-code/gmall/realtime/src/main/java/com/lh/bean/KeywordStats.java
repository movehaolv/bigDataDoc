package com.lh.bean;

import lombok.Data;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 16:29 2022/11/16
 */


@Data
public class KeywordStats {

    String keyword;
    Long ct;
    String source;
    String stt;
    String edt;
    Long ts;

}


//KeywordStats(keyword=口红, ct=2, source=search, stt=2022-11-10 17:51:50, edt=2022-11-10 17:52:00, ts=1668576095000)
