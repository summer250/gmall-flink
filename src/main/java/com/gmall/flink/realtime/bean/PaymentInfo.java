package com.gmall.flink.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * Desc: 支付实体类
 **/
@Data
public class PaymentInfo {
    Long id;  //编号
    Long order_id;  //订单编号
    Long user_id;  //用户ID
    BigDecimal total_amount;  //支付金额
    String subject;  //交易内容
    String payment_type;  //支付状态
    String create_time;  //创建时间
    String callback_time;  //回调时间
}
