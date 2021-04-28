package com.gmall.flink.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gmall.flink.realtime.app.function.DimAsyncFunction;
import com.gmall.flink.realtime.bean.OrderDetail;
import com.gmall.flink.realtime.bean.OrderInfo;
import com.gmall.flink.realtime.bean.OrderWide;
import com.gmall.flink.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Desc: 处理订单宽表
 * 业务执行流程
 * -模拟生成数据
 * -数据插入到MySQL中
 * -在binlog中记录数据的变化
 * -Maxwell将变化以json的形式发送到kafka的ODS(ods_base_db_maxwell)
 * -BaseDSApp读取ods_base_db_maxwell中的数据进行分流
 * >从MySQL配置表中读取数据
 * >将配置表缓存到map集合中
 * >检查Phoenix中的表是否存在
 * >对数据进行分流发送到不同的dwd层主题
 * -OrderWideApp从DWD的订单和订单明细读取数据
 **/
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置checkpoint
        //每5000s开始一次checkpoint,模式是EXACTLY_ONCE(默认)
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("file:///Volumes/BigData/flink/gmall_flink-realtime/src/main/resources/checkpoint"));

        //TODO 2.从kafka的DWD层读取订单和订单明细数据
        //2.1 声明相关的主题以及消费者组
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        //2.2 读取订单主题数据
        FlinkKafkaConsumer<String> orderInfoSource = MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId);
        DataStreamSource<String> orderInfoJsonStrDS = env.addSource(orderInfoSource);

        //2.3 读取订单明细数据
        FlinkKafkaConsumer<String> orderDetailSource = MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId);
        DataStreamSource<String> orderDetailJsonStrDS = env.addSource(orderDetailSource);

        //TODO 3.对读取的数据进行结构转换  jsonString --> OrderInfo|OrderDetail
        //3.1 转换订单结构
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoJsonStrDS.map(
                new RichMapFunction<String, OrderInfo>() {

                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderInfo map(String jsonStr) throws Exception {
                        OrderInfo orderInfo = JSON.parseObject(jsonStr, OrderInfo.class);
                        orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
                        return orderInfo;
                    }
                }
        );

        //3.2 转换订单明细结构
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailJsonStrDS.map(
                new RichMapFunction<String, OrderDetail>() {

                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderDetail map(String jsonStr) throws Exception {
                        OrderDetail orderDetail = JSON.parseObject(jsonStr, OrderDetail.class);
                        orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                        return orderDetail;
                    }
                }
        );

//        orderInfoDS.print("orderInfo>>>>>");
//        orderDetailDS.print("orderDetail>>>>>");

        //TODO 4.指定事件时间字段
        //4.1 订单指定事件时间字段
        SingleOutputStreamOperator<OrderInfo> orderInfoWithTsDS = orderInfoDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo orderInfo, long recordTimestamp) {
                                return orderInfo.getCreate_ts();
                            }
                        })
        );
        //4.2 订单明细指定事件时间字段
        SingleOutputStreamOperator<OrderDetail> orderDetailWithTsDS = orderDetailDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail orderDetail, long recordTimestamp) {
                                return orderDetail.getCreate_ts();
                            }
                        })
        );

        //TODO 5.按照订单id进行分组 指定关联的key
        KeyedStream<OrderInfo, Long> orderInfoKeyedDS = orderInfoWithTsDS.keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, Long> orderDetailKeyedDS = orderDetailWithTsDS.keyBy(OrderDetail::getOrder_id);

        //TODO 6.使用intervalJoin对订单和订单明细进行关联
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoKeyedDS
                .intervalJoin(orderDetailKeyedDS)
                .between(Time.milliseconds(-5), Time.milliseconds(5))
                .process(
                        new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                            @Override
                            public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                                out.collect(new OrderWide(orderInfo, orderDetail));
                            }
                        }
                );

//        orderWideDS.print("orderWide>>>>>");

        //TODO 7.关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(
                orderWideDS,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {

                    @Override
                    public String getKey(OrderWide OrderWide) {
                        return OrderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfJsonObj) throws Exception {
                        //获取用户生日
                        String birthday = dimInfJsonObj.getString("BIRTHDAY");
                        //定义日期转换工具类
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        //将生日字符串转换为生日日期对象
                        Date birthdayDate = sdf.parse(birthday);
                        //获取生日日期的毫秒数
                        Long birthdayTs = birthdayDate.getTime();

                        //获取当前时间的毫秒数
                        Long curTs = System.currentTimeMillis();

                        long ageTs = curTs - birthdayTs;
                        //转换为年龄
                        long ageLong = ageTs / 1000L / 60L / 60L / 24L / 365L;
                        Integer age = (int) ageLong;

                        //将维度中的年龄赋值给订单宽表中的属性
                        orderWide.setUser_age(age);

                        ////将维度中的性别赋值给订单宽表中的属性
                        String sex = dimInfJsonObj.getString("GENDER");
                        if (sex.equals("M")) {
                            orderWide.setUser_gender("男");
                        } else {
                            orderWide.setUser_gender("女");
                        }
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 8.关联省市维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(
                orderWideWithUserDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfJsonObj) throws Exception {
                        orderWide.setProvince_name(dimInfJsonObj.getString("NAME"));
                        orderWide.setProvince_area_code(dimInfJsonObj.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(dimInfJsonObj.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(dimInfJsonObj.getString("ISO_3166_2"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

//        orderWideWithProvinceDS.print(">>>>>>>>");

        //TODO 9.关联sku维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfJsonObj) throws Exception {
                        orderWide.setSku_name(dimInfJsonObj.getString("SKU_NAME"));
                        orderWide.setSpu_id(dimInfJsonObj.getLong("SPU_ID"));
                        orderWide.setCategory3_id(dimInfJsonObj.getLong("CATEGORY3_ID"));
                        orderWide.setTm_id(dimInfJsonObj.getLong("TM_ID"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

//        orderWideWithSkuDS.print(">>>>>>>>");

        //TODO 9.关联spu维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS,
                new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSpu_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfJsonObj) throws Exception {
                        orderWide.setSpu_name(dimInfJsonObj.getString("SPU_NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

//        orderWideWithSpuDS.print(">>>>>>>>>");

        //TODO 9.关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfJsonObj) throws Exception {
                        orderWide.setCategory3_name(dimInfJsonObj.getString("NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 9.关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithCategory3DS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getTm_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfJsonObj) throws Exception {
                        orderWide.setTm_name(dimInfJsonObj.getString("TM_NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

//        orderWideWithTmDS.print(">>>>>");

        //TODO 11.将关联以后的订单宽表数据写回到kafka的DWM层
        orderWideWithTmDS
                .map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSlink(orderWideSinkTopic));


        env.execute();
    }
}
