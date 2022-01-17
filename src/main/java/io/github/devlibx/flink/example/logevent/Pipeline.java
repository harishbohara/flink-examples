package io.github.devlibx.flink.example.logevent;

import io.gitbub.devlibx.easy.helper.common.LogEvent;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.flink.example.pojo.FlattenLogEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class Pipeline {

    /**
     * Processor pipeline
     */
    SingleOutputStreamOperator<FlattenLogEvent> process(StreamExecutionEnvironment env, DataStream<LogEvent> events, StringObjectMap parameter) {

        // Ensure that we do not send bad events
        DataStream<LogEvent> filteredEvent = events.filter(le -> le.getEntity() != null);

        // Collect items for last 60(n) sec, and emit this data every 5(n) sec
        return filteredEvent
                // Order by status - you can ignore it and use WindowAll function if required
                .keyBy(logEvent -> logEvent.getEntity().getType() + "-" + logEvent.getEntity().getId())
                // Setup event processor
                .process(new InternalProcessor())
                .name("LogEventToFlattenLogEventConvertor");
    }
}