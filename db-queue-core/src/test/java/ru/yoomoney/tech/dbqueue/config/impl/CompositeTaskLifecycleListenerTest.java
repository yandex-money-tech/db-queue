package ru.yoomoney.tech.dbqueue.config.impl;

import org.junit.Test;
import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.config.TaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class CompositeTaskLifecycleListenerTest {

    public static final QueueShardId SHARD_ID = new QueueShardId("shardId1");
    public static final QueueLocation LOCATION = QueueLocation.builder()
            .withTableName("table1").withQueueId(new QueueId("queueId1")).build();
    public static final TaskRecord TASK_RECORD = TaskRecord.builder().build();

    @Test
    public void should_handle_picked_in_order() {
        List<String> events = new ArrayList<>();
        StubTaskLifecycleListener firstListener = new StubTaskLifecycleListener("1", events);
        StubTaskLifecycleListener secondListener = new StubTaskLifecycleListener("2", events);
        CompositeTaskLifecycleListener compositeListener = new CompositeTaskLifecycleListener(
                Arrays.asList(firstListener, secondListener));
        compositeListener.picked(SHARD_ID, LOCATION, TASK_RECORD, 42L);
        assertThat(events, equalTo(Arrays.asList("1:picked", "2:picked")));
    }

    @Test
    public void should_handle_started_in_order() {
        List<String> events = new ArrayList<>();
        StubTaskLifecycleListener firstListener = new StubTaskLifecycleListener("1", events);
        StubTaskLifecycleListener secondListener = new StubTaskLifecycleListener("2", events);
        CompositeTaskLifecycleListener compositeListener = new CompositeTaskLifecycleListener(
                Arrays.asList(firstListener, secondListener));
        compositeListener.started(SHARD_ID, LOCATION, TASK_RECORD);
        assertThat(events, equalTo(Arrays.asList("1:started", "2:started")));
    }

    @Test
    public void should_handle_executed_in_order() {
        List<String> events = new ArrayList<>();
        StubTaskLifecycleListener firstListener = new StubTaskLifecycleListener("1", events);
        StubTaskLifecycleListener secondListener = new StubTaskLifecycleListener("2", events);
        CompositeTaskLifecycleListener compositeListener = new CompositeTaskLifecycleListener(
                Arrays.asList(firstListener, secondListener));
        compositeListener.executed(SHARD_ID, LOCATION, TASK_RECORD, TaskExecutionResult.finish(), 42L);
        assertThat(events, equalTo(Arrays.asList("1:executed", "2:executed")));
    }

    @Test
    public void should_handle_finished_in_order() {
        List<String> events = new ArrayList<>();
        StubTaskLifecycleListener firstListener = new StubTaskLifecycleListener("1", events);
        StubTaskLifecycleListener secondListener = new StubTaskLifecycleListener("2", events);
        CompositeTaskLifecycleListener compositeListener = new CompositeTaskLifecycleListener(
                Arrays.asList(firstListener, secondListener));
        compositeListener.finished(SHARD_ID, LOCATION, TASK_RECORD);
        assertThat(events, equalTo(Arrays.asList("1:finished", "2:finished")));
    }

    @Test
    public void should_handle_crashed_in_order() {
        List<String> events = new ArrayList<>();
        StubTaskLifecycleListener firstListener = new StubTaskLifecycleListener("1", events);
        StubTaskLifecycleListener secondListener = new StubTaskLifecycleListener("2", events);
        CompositeTaskLifecycleListener compositeListener = new CompositeTaskLifecycleListener(
                Arrays.asList(firstListener, secondListener));
        compositeListener.crashed(SHARD_ID, LOCATION, TASK_RECORD, null);
        assertThat(events, equalTo(Arrays.asList("1:crashed", "2:crashed")));
    }

    public static class StubTaskLifecycleListener implements TaskLifecycleListener {

        private final String id;
        private final List<String> events;

        public StubTaskLifecycleListener(@Nonnull String id, @Nonnull List<String> events) {
            this.id = Objects.requireNonNull(id);
            this.events = Objects.requireNonNull(events);
        }

        @Override
        public void picked(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord, long pickTaskTime) {
            events.add(id + ":picked");
        }

        @Override
        public void started(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord) {
            events.add(id + ":started");
        }

        @Override
        public void executed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord, @Nonnull TaskExecutionResult executionResult, long processTaskTime) {
            events.add(id + ":executed");
        }

        @Override
        public void finished(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord) {
            events.add(id + ":finished");
        }

        @Override
        public void crashed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord, @Nullable Exception exc) {
            events.add(id + ":crashed");
        }
    }
}