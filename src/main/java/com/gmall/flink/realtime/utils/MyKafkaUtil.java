package com.gmall.flink.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * Desc: 操作kafka的工具类
 **/
public class MyKafkaUtil {

    private static String KAFKA_SERVER = "localhost:9092";
    private static String DEFAULT_TOPIC = "DEFAULT_DATA";

    //获取flinkKafkaConsumer
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        //Kafka连接的一些属性配置
        Properties propes = new Properties();
        propes.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        propes.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);

        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), propes);
    }

    //封装FlinkKafkaProducer
    public static FlinkKafkaProducer<String> getKafkaSlink(String topic) {
        return new FlinkKafkaProducer<String>(KAFKA_SERVER, topic, new SimpleStringSchema());
    }

    public static <T> FlinkKafkaProducer<T> getKafkaSlinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        //kafka基本配置信息
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        //指定生产数据超时时间
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "");
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC, kafkaSerializationSchema, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

}
