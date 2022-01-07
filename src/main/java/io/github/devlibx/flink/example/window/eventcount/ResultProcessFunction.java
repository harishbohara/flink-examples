package io.github.devlibx.flink.example.window.eventcount;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.joda.time.DateTime;

public class ResultProcessFunction extends KeyedProcessFunction<String, Integer, Alert> {
    private final int slide;
    private long nextTimeStamp;

    public ResultProcessFunction(int slide) {
        this.slide = slide;
    }

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
        // --> remove this if condition
        Alert alert = new Alert();
        alert.setId(count);
        out.collect(alert);

        ctx.timerService().deleteProcessingTimeTimer(nextTimeStamp);
        nextTimeStamp = DateTime.now().plusSeconds(slide + 10).getMillis();
        ctx.timerService().registerProcessingTimeTimer(nextTimeStamp);
    }
}
