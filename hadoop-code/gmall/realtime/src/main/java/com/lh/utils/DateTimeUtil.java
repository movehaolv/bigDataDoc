package com.lh.utils;

import com.lh.bean.ProductStats;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashSet;

public class DateTimeUtil {

    private final static DateTimeFormatter formater = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static String toYMDhms(Date date) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return formater.format(localDateTime);
    }

    public static Long toTs(String YmDHms) {
        LocalDateTime localDateTime = LocalDateTime.parse(YmDHms, formater);
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    public static void main(String[] args) throws ParseException {

        HashSet<Long> orderIds = new HashSet<>();
        orderIds.add(12L);
        ProductStats build = ProductStats.builder()
                .refundOrderIdSet(orderIds)
                .build();
        ProductStats build1 = ProductStats.builder()
                .refundOrderIdSet(orderIds)
                .build();

        HashSet<Long> orderIds1 = new HashSet<>();
        orderIds1.add(13L);
        orderIds1.add(15L);
        build1.setRefundOrderIdSet(orderIds1);

        build.getRefundOrderIdSet().addAll(build1.getRefundOrderIdSet());
        System.out.println(build.getRefundOrderIdSet());
        System.out.println(build1.getRefundOrderIdSet());

    }
}

