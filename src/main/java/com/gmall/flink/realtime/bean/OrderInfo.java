package com.gmall.flink.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * Desc: 订单实体类
 **/
@Data
public class OrderInfo {
    Long id;  //编号
    Long province_id;  //地区
    String order_status;  //订单状态
    Long user_id;  //用户id
    BigDecimal total_amount;  //总金额
    BigDecimal activity_reduce_amount;  //促销金额
    BigDecimal coupon_reduce_amount;  //优惠券
    BigDecimal original_total_amount;  //原价金额
    BigDecimal feight_fee;  //运费
    String expire_time;  //失效时间
    String create_time;  //创建时间
    String operate_time;  //操作时间
    String create_date; // 把其他字段处理得到
    String create_hour;
    Long create_ts;
}
