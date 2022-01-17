package io.github.devlibx.flink.example.window.eventcount;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.gitbub.devlibx.easy.helper.yaml.YamlUtils;
import io.github.devlibx.easy.flink.functions.common.EventCount;
import io.github.devlibx.easy.messaging.config.MessagingConfigs;
import io.github.devlibx.easy.messaging.consumer.IConsumer;
import io.github.devlibx.easy.messaging.kafka.module.MessagingKafkaModule;
import io.github.devlibx.easy.messaging.module.MessagingModule;
import io.github.devlibx.easy.messaging.producer.IProducer;
import io.github.devlibx.easy.messaging.service.IMessagingFactory;
import io.github.devlibx.flink.common.KafkaMessagingTestConfig;
import io.github.devlibx.flink.common.TestConfigUtil;
import io.github.devlibx.flink.example.pojo.Order;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class EventCountJobTest {
    private static IProducer producer;
    private static IConsumer consumer;

    @BeforeAll
    public static void setup() {
        // Read config from file
        KafkaMessagingTestConfig kafkaConfig = YamlUtils.readYaml("messaging.yaml", KafkaMessagingTestConfig.class);
        kafkaConfig.messaging.getConsumers().get("orders").put("group.id", UUID.randomUUID().toString());

        Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(MessagingConfigs.class).toInstance(kafkaConfig.messaging);
            }
        }, new MessagingKafkaModule(), new MessagingModule());

        // Get messaging factory and initialize the factory
        IMessagingFactory messagingFactory = injector.getInstance(IMessagingFactory.class);
        messagingFactory.initialize();

        producer = messagingFactory.getProducer("orders").get();
        consumer = messagingFactory.getConsumer("orders").get();
    }

    @Test
    @Disabled
    public void testPipeline() throws Exception {
        // Test UUID
        String uid = UUID.randomUUID().toString();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<EventCount> eventCount = new AtomicReference<>();
        EventCountJobSub.sinkFunction = new SinkFunction<EventCount>() {
            @Override
            public void invoke(EventCount value, Context context) throws Exception {
                eventCount.set(value);
                latch.countDown();
            }
        };

        // Start the job
        new Thread(() -> {
            try {
                Path currentRelativePath = Paths.get("");
                String path = currentRelativePath.toAbsolutePath().toString();
                String file = TestConfigUtil.cloneConfig(path + "/config.properties", UUID.randomUUID().toString());
                EventCountJob.main(new String[]{"--config", file});
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        // Publish 100 events
        Thread.sleep(10000);
        new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                Order order = new Order();
                order.setOrderStatus("F");
                order.setOrderKey(i);
                order.setRowNumber(UUID.randomUUID().toString());
                order.setTotalPrice(10);
                order.setOrderDate(DateTime.now().toString());
                order.setClerk(UUID.randomUUID().toString());
                order.setShipPriority(1);
                order.setComment("comment1");
                producer.send(i + "", JsonUtils.asJson(order).getBytes());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
            }
        }).start();

        latch.await(60, TimeUnit.MINUTES);
        Assertions.assertNotNull(eventCount.get());
        Assertions.assertTrue(eventCount.get().getCount() > 0);
    }

    private static class EventCountJobSub extends EventCountJob {
        private static SinkFunction<EventCount> sinkFunction;

        @Override
        protected SinkFunction<EventCount> sink() {
            return sinkFunction;
        }
    }
}
