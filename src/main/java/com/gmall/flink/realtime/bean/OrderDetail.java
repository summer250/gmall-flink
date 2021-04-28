package com.gmall.flink.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * Desc: 订单明细实体类
 **/
@Data
public class OrderDetail {
    Long id;  //编号
    Long order_id;  //订单编号
    Long sku_id;  //sku_id
    BigDecimal order_price;  //购买价格(下单时sku价格）
    Long sku_num;  //购买个数
    String sku_name;  //sku名称（冗余)
    String create_time;  //创建时间
    BigDecimal split_total_amount;
    BigDecimal split_activity_amount;
    BigDecimal split_coupon_amount;
    Long create_ts;
}
