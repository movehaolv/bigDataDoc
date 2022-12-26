package com.lh.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.sql.Date;

/**
 * Desc:地区统计宽表实体类
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class ProvinceStats {

    private String stt;
    private String edt;

    private Long province_id;
    private String province_name;
    private String province_area_code;
    private String province_iso_code;
    private String province_3166_2_code;
    private BigDecimal total_amount;
    private BigDecimal split_total_amount;
    private BigDecimal feight_fee;
    private Long order_count;
    private Long ts;

}