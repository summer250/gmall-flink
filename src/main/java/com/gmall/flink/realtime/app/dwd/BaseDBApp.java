package com.gmall.flink.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gmall.flink.realtime.app.function.DimSink;
import com.gmall.flink.realtime.app.function.TableProcessFunction;
import com.gmall.flink.realtime.bean.TableProcess;
import com.gmall.flink.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * Desc: 准备业务数据的DWD层
 **/
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //配置checkpoint
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("file:///Volumes/BigData/flink/gmall_flink-realtime/src/main/resources/checkpoint"));
//        //重启策略
//        // 如果说没有开启重启checkpoint,那么重启策略就是noRestart
//        // 如果说没有开启checkpoint,那么重启策略就会尝试自动帮你进行重启 重启Integer.MaxValue
//        env.setRestartStrategy(RestartStrategies.noRestart());

        //从卡夫卡的ODS层读取数据
        String topic = "ods_base_db_maxwell";
        String groupId = "base_log_app_group";

        //通过工具类获取kafka的消费者
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> jsonStrDS = env.addSource(kafkaSource);

        //结构转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map((MapFunction<String, JSONObject>) jsonStr -> JSON.parseObject(jsonStr));

        //TODO 对数据进行ETL     如果data为空,或者长度<3,将这样的数据过滤掉
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                jsonObj -> {
                    boolean flag = jsonObj.getString("table") != null &&
                            jsonObj.getJSONObject("data") != null &&
                            jsonObj.getString("data").length() >= 3;
                    return flag;
                }
        );


//        filterDS.print("json>>>>");

        //TODO 3.动态分流  事实表放到主流,写回到kafka的DWD层,如果维度表,通过侧输出流,写入到hbase
        //3.1定义输出到Hbase的侧输出流标签
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {
        };

        //3.2 主流    写回到kafka的数据
        SingleOutputStreamOperator<JSONObject> kafkaDS = filterDS.process(new TableProcessFunction(hbaseTag));

        //3.3 获取侧输出流    写到hbase的数据
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(hbaseTag);

        kafkaDS.print("事实>>>>>>");
        hbaseDS.print("维度>>>>>>");

        //TODO 4.将维度数据保存到Phoenix对应的维度表中
        hbaseDS.addSink(new DimSink());

        //TODO 5.将事实数据写回到kafka的dwd层
        FlinkKafkaProducer<JSONObject> kafkaSlink = MyKafkaUtil.getKafkaSlinkBySchema(
                new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public void open(SerializationSchema.InitializationContext context) throws Exception {
                        System.out.println("kafka序列化");
                    }

                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, @Nullable Long aLong) {
                        String slinkTopic = jsonObj.getString("sink_table");
                        JSONObject dataJsonObj = jsonObj.getJSONObject("data");
                        return new ProducerRecord<>(slinkTopic, dataJsonObj.toString().getBytes());
                    }
                }
        );

        kafkaDS.addSink(kafkaSlink);

        env.execute();
    }
}

