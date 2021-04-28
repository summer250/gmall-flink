package com.gmall.flink.realtime.app.dwm;


import com.alibaba.fastjson.JSON;
import com.gmall.flink.realtime.bean.OrderWide;
import com.gmall.flink.realtime.bean.PaymentInfo;
import com.gmall.flink.realtime.bean.PaymentWide;
import com.gmall.flink.realtime.utils.DateTimeUtil;
import com.gmall.flink.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Desc: 支付宽表处理程序
 **/
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置checkpoint
        //每5000s开始一次checkpoint,模式是EXACTLY_ONCE(默认)
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("file:///Volumes/BigData/flink/gmall_flink-realtime/src/main/resources/checkpoint"));

        //TODO 2.从kafka的DWD层读取数据
        //2.1 声明相关的主题以及消费者组
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        String groupId = "paymentwide_app_group";

        //2.2 读取支付数据
        FlinkKafkaConsumer<String> paymentInfoSource = MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId);
        DataStreamSource<String> paymentInfoJsonStrDS = env.addSource(paymentInfoSource);

        //2.3 读取订单宽表数据
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        DataStreamSource<String> orderWideJsonStrDS = env.addSource(orderWideSource);

        //TODO 3.对读取到的数据进行结构的转换  jsonStr -> POJO
        //3.1 转换支付流
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentInfoJsonStrDS.map(jsonStr -> JSON.parseObject(jsonStr, PaymentInfo.class));
        //3.2 转换订单流
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideJsonStrDS.map(jsonStr -> JSON.parseObject(jsonStr, OrderWide.class));

//        paymentInfoDS.print("支付>>>>");
//        orderWideDS.print("订单宽表>>>>");

        //TODO 4.设置watermark以及提取事件时间字段
        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithWatermarkDS = paymentInfoDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<PaymentInfo>() {
                                    @Override
                                    public long extractTimestamp(PaymentInfo paymentInfo, long recordTimestamp) {
                                        //需要将字符串的时间转换为毫秒数
                                        return DateTimeUtil.toTs(paymentInfo.getCallback_time());
                                    }
                                }
                        )
        );
        //4.2 订单流的Watermark
        SingleOutputStreamOperator<OrderWide> orderWideWithWatermarkDS = orderWideDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderWide>() {
                                    @Override
                                    public long extractTimestamp(OrderWide orderWide, long recordTimestamp) {
                                        return DateTimeUtil.toTs(orderWide.getCreate_time());
                                    }
                                }
                        )
        );

        //TODO 5.对数据进行分组
        //5.1 支付流数据分组
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedDS = paymentInfoWithWatermarkDS.keyBy(PaymentInfo::getOrder_id);
        //5.2 订单宽表流数据分组
        KeyedStream<OrderWide, Long> orderWideKeyedDS = orderWideWithWatermarkDS.keyBy(OrderWide::getOrder_id);

        //TODO 6.使用IntervalJoin关联两条流
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoKeyedDS
                .intervalJoin(orderWideKeyedDS)
                .between(Time.seconds(-1800), Time.seconds(0))
                .process(
                        new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                            @Override
                            public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                                out.collect(new PaymentWide(paymentInfo, orderWide));
                            }
                        }
                );

        paymentWideDS.print(">>>>");
        //TODO 7.将数据写到kafka的dwm层
        paymentWideDS
                .map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSlink(paymentWideSinkTopic));


        env.execute();
    }
}
