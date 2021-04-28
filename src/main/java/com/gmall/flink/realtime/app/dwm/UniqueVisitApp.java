package com.gmall.flink.realtime.app.dwm;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.gmall.flink.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Desc: 独立访客UV计算
 * 前期准备:
 * -启动ZK,kafka,Logger.sh,BaseLogApp,UniqueVisitApp
 * 执行流程:
 * 模式生成日志的jar->nginx->日志采集服务->kafka(ods)
 * ->BaseLogApp(分流)->kafka(dwd) dwd_page_log
 * ->UniqueVisitApp(独立访客)->kafka(dwm_unique_visit)
 **/
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //设置checkpoint
        //每5000s开始一次checkpoint,模式是EXACTLY_ONCE(默认)
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("file:///Volumes/BigData/flink/gmall_flink-realtime/src/main/resources/checkpoint"));

        //2.从kafka中读取数据
        String sourceTopic = "dwd_page_log";
        String groupId = "UniqueVisitApp_group";
        String slinkTopic = "dwm_unique_visit";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> jsonStrDS = env.addSource(kafkaSource);

        //TODO 3.对读取到的数据进行结构转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(JSON::parseObject);

//        jsonObjDS.print(">>>>>");

        //TODO 4.按照设备id进行分组
        KeyedStream<JSONObject, String> keybyWithMidDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        //过滤得到UV
        SingleOutputStreamOperator<JSONObject> filteredDS = keybyWithMidDS.filter(
                new RichFilterFunction<JSONObject>() {
                    //定义状态
                    ValueState<String> lastVisitDateState = null;
                    //定义日期工具类
                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //初始化日期工具类
                        sdf = new SimpleDateFormat("yyyyMMdd");
                        //初始化状态
                        ValueStateDescriptor<String> lastVisitDateStateDS = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                        //因为我们统计的是日活DAU,所以状态数据只在当天有效,过了一天就可以失效掉
                        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1)).build();
                        lastVisitDateStateDS.enableTimeToLive(stateTtlConfig);
                        this.lastVisitDateState = getRuntimeContext().getState(lastVisitDateStateDS);
                    }

                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        //首先判断当前页面是否是从别的页面跳转过来的
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (lastPageId != null && lastPageId.length() > 0) {
                            return false;
                        }

                        //获取当前访问时间
                        Long ts = jsonObj.getLong("ts");
                        //将当前访问时间戳转换为日期字符串
                        String logDate = sdf.format(new Date(ts));
                        //获取状态日期
                        String lastVisitDate = lastVisitDateState.value();

                        //用当前页面的访问时间和状态时间进行对比
                        if (lastVisitDate != null && lastVisitDate.length() > 0 && lastVisitDate.equals(logDate)) {
                            System.out.println("已访问,lastVisitDate--" + lastVisitDate + "||logDate:" + logDate);
                            return false;
                        } else {
                            System.out.println("未访问,lastVisitDate--" + lastVisitDate + "||logDate:" + logDate);
                            lastVisitDateState.update(logDate);
                            return true;
                        }
                    }
                }
        );

//        filteredDS.print(">>>>>>>>>");
        //TODO 6.向kafka中写回,需要将json转换为string

        //6.1 json->string
        SingleOutputStreamOperator<String> kafkaDS = filteredDS.map(JSONAware::toJSONString);

        //6.2 写回到kafka的DWM层
        kafkaDS.addSink(MyKafkaUtil.getKafkaSlink(slinkTopic));
        env.execute();
    }
}
