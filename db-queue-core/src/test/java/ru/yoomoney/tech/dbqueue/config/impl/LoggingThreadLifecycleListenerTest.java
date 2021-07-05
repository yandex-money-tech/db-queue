package ru.yoomoney.tech.dbqueue.config.impl;

import org.junit.Before;
import org.junit.Test;
import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

public class LoggingThreadLifecycleListenerTest {

    public static final Path LOG_PATH = Paths.get("target/thread-listener.log");

    @Before
    public void setUp() throws Exception {
        if (Files.exists(LOG_PATH)) {
            Files.write(LOG_PATH, new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
        }
    }

    @Test
    public void should_log_thread_lifecycle() throws IOException {
        LoggingThreadLifecycleListener listener = new LoggingThreadLifecycleListener();
        QueueShardId shardId = new QueueShardId("shardId1");
        QueueLocation location = QueueLocation.builder()
                .withTableName("table1").withQueueId(new QueueId("queueId1")).build();
        listener.started(shardId, location);
        listener.executed(shardId, location, true, 42L);
        listener.finished(shardId, location);
        listener.crashed(shardId, location, null);

        List<String> logFile = Files.readAllLines(LOG_PATH);
        assertThat(logFile, equalTo(Collections.singletonList("ERROR [main] [LoggingThreadLifecycleListener] fatal error in queue thread: shardId=shardId1, location={id=queueId1,table=table1}")));
    }
}