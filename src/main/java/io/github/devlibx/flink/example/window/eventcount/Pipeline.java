package io.github.devlibx.flink.example.window.eventcount;

import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.window.CountAggregatorFunction;
import io.github.devlibx.flink.example.pojo.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.joda.time.DateTime;

@Slf4j
public class Pipeline {


    SingleOutputStreamOperator<Alert> process(StreamExecutionEnvironment env, DataStream<Order> orders, StringObjectMap parameter) {

        // FOR CLIENT TO CHANGE - Filter orders
        // Let's say you want to filter only failed orders.
        DataStream<Order> highPriorityOrders = orders
                .filter(order -> "F".equals(order.getOrderStatus()));

        // Get the window size and slide from config
        int size = parameter.getInt("windowDuration", 60);
        int slide = parameter.getInt("emitResultEventNthSeconds", 5);

        // Collect items for last 60(n) sec, and emit this data every 5(n) sec
        return highPriorityOrders

                // Order by status - you can ignore it and use WindowAll function if required
                .keyBy(Order::getOrderStatus)

                // Collect window in a sliding window
                .window(SlidingEventTimeWindows.of(Time.seconds(size), Time.seconds(slide)))

                // Sum all the orders in this window
                .aggregate(new CountAggregatorFunction<>())

                .keyBy(value -> "1")

                // Finally output the result at the end of each slide
                .process(new KeyedProcessFunction<String, Integer, Alert>() {
                    long nextTimeStamp;

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);

                        // Send a alert that we did not get events in last window
                        Alert alert = new Alert();
                        alert.setId(-1);
                        out.collect(alert);

                        ctx.timerService().deleteProcessingTimeTimer(timestamp);
                        nextTimeStamp = DateTime.now().plusSeconds(slide + 10).getMillis();
                        ctx.timerService().registerProcessingTimeTimer(nextTimeStamp);
                    }

                    @Override
                    public void processElement(Integer count, Context ctx, Collector<Alert> out) throws Exception {

                        // CLIENT TO UNCOMMENT - You can send count like following
                        // For this sample I want to send a alert if I did not get event in N sec
                        // You uncomment the code to send event
                        if (false) { // --> remove this if condition
                            Alert alert = new Alert();
                            alert.setId(count);
                            out.collect(alert);
                        }

                        ctx.timerService().deleteProcessingTimeTimer(nextTimeStamp);
                        nextTimeStamp = DateTime.now().plusSeconds(slide + 10).getMillis();
                        ctx.timerService().registerProcessingTimeTimer(nextTimeStamp);
                    }
                })
                .name("OrderAggregator");
    }
}


