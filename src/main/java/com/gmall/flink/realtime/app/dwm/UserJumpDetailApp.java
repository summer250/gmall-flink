package com.gmall.flink.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gmall.flink.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.RichPatternFlatSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * Desc: 用户跳出行为过滤
 **/
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //设置checkpoint
        //每5000s开始一次checkpoint,模式是EXACTLY_ONCE(默认)
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("file:///Volumes/BigData/flink/gmall_flink-realtime/src/main/resources/checkpoint"));

        //2.从kafka中读取数据
        String sourceTopic = "dwd_page_log";
        String groupId = "user_jump_detail_group";
        String slinkTopic = "dwm_user_jump_detail";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> jsonStrDS = env.addSource(kafkaSource);

        //TODO 3.对读取到的数据进行结构的转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(JSON::parseObject);

//        jsonObjDS.print("json>>>>>");

        //TODO 4.指定事件时间
        SingleOutputStreamOperator<JSONObject> jsonObjWithTSDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                return jsonObj.getLong("ts");
                            }
                        }
                ));

        //TODO 5.按照mid进行分组
        KeyedStream<JSONObject, String> keyByMidDS = jsonObjWithTSDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        /*
            计算页面跳出明细,需要满足两个条件
                1.不是从其他页面跳转过来的页面,是一个首次访问页面
                                last_page_id == null
                2.具体首次访问结束后10s内,没有对其他的页面再进行访问

         */
        //TODO 6.配置CEP表达式
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first")
                .where(
                        //条件1:不是从其他页面跳转过来的页面,是一个首次访问页面
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject jsonObj) throws Exception {
                                //获取last_page_id
                                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                                //判断是否为null,将为空的保留,非空的过滤掉
                                if (lastPageId == null || lastPageId.length() == 0) {
                                    return true;
                                }
                                return false;
                            }
                        }
                )
                .next("next")
                .where(
                        //条件2:具体首次访问结束后10s内,没有对其他的页面再进行访问
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject jsonObj) throws Exception {
                                //获取当前页面的id
                                String pageId = jsonObj.getJSONObject("page").getString("page_id");
                                //判断当前访问的页面id是否为null
                                if (pageId != null && pageId.length() > 0) {
                                    return true;
                                }
                                return false;
                            }
                        }
                )
                //时间限制模式
                .within(Time.milliseconds(10000));

        //TODO 7.根据CEP表达式筛选流
        PatternStream<JSONObject> patternStream = CEP.pattern(keyByMidDS, pattern);

        //TODO 8.从筛选之后的流中,提取数据   将超时数据 放到侧输出流中
        OutputTag<String> timeoutTag = new OutputTag<String>("timeout") {
        };

        SingleOutputStreamOperator<String> filterDS = patternStream.flatSelect(
                timeoutTag,
                //处理超时数据
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                        //获取所有符合first的json对象
                        List<JSONObject> jsonObjectList = pattern.get("first");
                        //注意:在timeout方法中的数据都会被参数1中的标签标记
                        for (JSONObject jsonObject : jsonObjectList) {
                            out.collect(jsonObject.toJSONString());
                        }
                    }
                },
                //处理没有超时数据
                new RichPatternFlatSelectFunction<JSONObject, String>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<String> out) throws Exception {
                        //没有超时的数据,不在我们的统计范围之内,所以不需要写代码
                    }
                }
        );

        //TODO 9.从侧输出流中获取超时数据
        DataStream<String> jumpDS = filterDS.getSideOutput(timeoutTag);

//        jumpDS.print(">>>>>>");

        //TODO 10.将跳出数据写回到kafka的DWM层
        jumpDS.addSink(MyKafkaUtil.getKafkaSlink(slinkTopic));


        env.execute();
    }
}
