package io.github.devlibx.flink.example.timer;

import io.github.devlibx.flink.utils.ConfigReader;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.walkthrough.common.entity.Alert;

public class Job {
    public static void main(String[] args) throws Exception {
        ParameterTool argsParams = ParameterTool.fromArgs(args);
        String bucket = argsParams.getRequired("bucket");
        String filePath = argsParams.getRequired("file");
        ParameterTool parameter = ConfigReader.readConfigsFromS3(bucket, filePath, false);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<Order> source = KafkaSource.<Order>builder()
                .setBootstrapServers(parameter.get("brokers", "localhost:9092"))
                .setTopics(parameter.get("topic", "orders"))
                .setGroupId(parameter.get("groupId", "1234"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new EventDeserializationSchema())
                .build();
        DataStream<Order> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        DataStream<Alert> input1 = input.keyBy(Order::getCustomerKey).process(new CustomProcessor());
        input1.addSink(new PrintSinkFunction<>());
        env.execute("Fraud Detection");
    }
}
