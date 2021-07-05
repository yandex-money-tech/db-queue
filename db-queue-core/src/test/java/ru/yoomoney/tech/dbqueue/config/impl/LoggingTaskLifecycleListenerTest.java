package ru.yoomoney.tech.dbqueue.config.impl;

import org.junit.Before;
import org.junit.Test;
import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class LoggingTaskLifecycleListenerTest {

    public static final QueueShardId SHARD_ID = new QueueShardId("shardId1");
    public static final QueueLocation LOCATION = QueueLocation.builder()
            .withTableName("table1").withQueueId(new QueueId("queueId1")).build();
    public static final TaskRecord TASK_RECORD = TaskRecord.builder()
            .withId(2L)
            .withCreatedAt(ZonedDateTime.ofInstant(Instant.EPOCH, ZoneId.of("Z")))
            .withAttemptsCount(10L)
            .withNextProcessAt(ZonedDateTime.ofInstant(Instant.EPOCH.plusSeconds(10), ZoneId.of("Z")))
            .build();
    public static final Path LOG_PATH = Paths.get("target/task-listener.log");

    @Before
    public void setUp() throws Exception {
        if (Files.exists(LOG_PATH)) {
            Files.write(LOG_PATH, new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
        }
    }

    @Test
    public void should_log_common_lifecycle() throws IOException {
        LoggingTaskLifecycleListener listener = new LoggingTaskLifecycleListener();
        listener.started(SHARD_ID, LOCATION, TASK_RECORD);
        listener.picked(SHARD_ID, LOCATION, TASK_RECORD, 42L);
        listener.finished(SHARD_ID, LOCATION, TASK_RECORD);
        listener.crashed(SHARD_ID, LOCATION, TASK_RECORD, null);

        List<String> logFile = Files.readAllLines(LOG_PATH);
        assertThat(logFile, equalTo(Arrays.asList(
                "INFO  [main] [LoggingTaskLifecycleListener] consuming task: id=2, attempt=10",
                "ERROR [main] [LoggingTaskLifecycleListener] error while processing task: task={id=2, attemptsCount=10, reenqueueAttemptsCount=0, totalAttemptsCount=0, createdAt=1970-01-01T00:00Z, nextProcessAt=1970-01-01T00:00:10Z}")));
    }

    @Test
    public void should_log_finish_result() throws IOException {
        Clock currentTime = Clock.fixed(Instant.EPOCH.plusSeconds(2), ZoneId.of("Z"));
        LoggingTaskLifecycleListener listener = new LoggingTaskLifecycleListener(currentTime);

        listener.executed(SHARD_ID, LOCATION, TASK_RECORD, TaskExecutionResult.finish(), 42L);

        List<String> logFile = Files.readAllLines(LOG_PATH);
        assertThat(logFile, equalTo(Collections.singletonList("INFO  [main] [LoggingTaskLifecycleListener] task finished: id=2, in-queue=PT2S, time=42")));
    }

    @Test
    public void should_log_fail_result() throws IOException {
        LoggingTaskLifecycleListener listener = new LoggingTaskLifecycleListener(Clock.systemDefaultZone());

        listener.executed(SHARD_ID, LOCATION, TASK_RECORD, TaskExecutionResult.fail(), 42L);

        List<String> logFile = Files.readAllLines(LOG_PATH);
        assertThat(logFile, equalTo(Collections.singletonList("INFO  [main] [LoggingTaskLifecycleListener] task failed: id=2, time=42")));
    }

    @Test
    public void should_log_reenqueue_result() throws IOException {
        LoggingTaskLifecycleListener listener = new LoggingTaskLifecycleListener(Clock.systemDefaultZone());

        listener.executed(SHARD_ID, LOCATION, TASK_RECORD, TaskExecutionResult.reenqueue(Duration.ofMinutes(1)), 42L);

        List<String> logFile = Files.readAllLines(LOG_PATH);
        assertThat(logFile, equalTo(Collections.singletonList("INFO  [main] [LoggingTaskLifecycleListener] task reenqueued: id=2, delay=PT1M, time=42")));
    }

}