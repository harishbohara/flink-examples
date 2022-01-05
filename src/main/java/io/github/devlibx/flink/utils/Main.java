package io.github.devlibx.flink.utils;

import com.google.common.base.Strings;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;

public class Main {
    public static void main(String[] args, RunJob runJob) throws Exception {

        // Read config from S3
        ParameterTool argsParams = ParameterTool.fromArgs(args);
        ParameterTool parameter = null;
        String url = argsParams.getRequired("config");
        if (url.startsWith("s3")) {
            parameter = ConfigReader.readConfigsFromS3(url, false);
        } else {
            throw new Exception("Only s3 url is supported in config");
        }

        // Run the input job
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        setupStore(env, parameter);
        runJob.run(env, parameter);
        env.execute("Fraud Detection");
    }

    public static void setupStore(StreamExecutionEnvironment env, ParameterTool parameter) {
        String checkpointDir = null;
        if (!Strings.isNullOrEmpty(parameter.get("checkpoint-dir"))) {
            checkpointDir = parameter.get("checkpoint-dir");
        } else if (!Strings.isNullOrEmpty(parameter.get("state.checkpoints.dir"))) {
            checkpointDir = parameter.get("state.checkpoints.dir");
        }
        if (!Strings.isNullOrEmpty(checkpointDir)) {
            env.getCheckpointConfig().setCheckpointStorage(checkpointDir);
        }

        String backend = null;
        if (!Strings.isNullOrEmpty(parameter.get("backend"))) {
            backend = parameter.get("backend");
        } else if (!Strings.isNullOrEmpty(parameter.get("state.backend"))) {
            backend = parameter.get("state.backend");
        }
        if (!Strings.isNullOrEmpty(backend) && Objects.equals("rocksdb", backend)) {
            env.setStateBackend(new EmbeddedRocksDBStateBackend());
        }
    }

    public interface RunJob {
        void run(StreamExecutionEnvironment env, ParameterTool parameter);
    }
}
