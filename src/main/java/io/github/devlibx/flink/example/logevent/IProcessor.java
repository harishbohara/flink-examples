package io.github.devlibx.flink.example.logevent;

import io.gitbub.devlibx.easy.helper.common.LogEvent;
import io.github.devlibx.flink.example.pojo.FlattenLogEvent;

public interface IProcessor {
    void process(LogEvent logEvent, FlattenLogEvent flattenLogEvent);
}
