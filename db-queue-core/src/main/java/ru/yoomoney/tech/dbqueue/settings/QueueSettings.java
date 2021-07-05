package ru.yoomoney.tech.dbqueue.settings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Queue settings
 *
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
public final class QueueSettings {

    private static final Function<String, Duration> DURATION_CONVERTER =
            Memoizer.memoize(Duration::parse);

    private static final Duration DEFAULT_TIMEOUT_ON_FATAL_CRASH = Duration.ofSeconds(1L);

    private final int threadCount;
    @Nonnull
    private final Duration noTaskTimeout;
    @Nonnull
    private final Duration betweenTaskTimeout;
    @Nonnull
    private final Duration fatalCrashTimeout;
    @Nonnull
    private final Duration retryInterval;
    @Nonnull
    private final TaskRetryType retryType;
    @Nonnull
    private final ReenqueueRetrySettings reenqueueRetrySettings;
    @Nonnull
    private final ProcessingMode processingMode;
    @Nonnull
    private final Map<String, String> additionalSettings;

    private QueueSettings(@Nonnull Duration noTaskTimeout,
                          @Nonnull Duration betweenTaskTimeout,
                          @Nullable Duration fatalCrashTimeout,
                          @Nullable Integer threadCount,
                          @Nullable TaskRetryType retryType,
                          @Nullable Duration retryInterval,
                          @Nullable ReenqueueRetrySettings reenqueueRetrySettings,
                          @Nullable ProcessingMode processingMode,
                          @Nullable Map<String, String> additionalSettings) {
        this.noTaskTimeout = Objects.requireNonNull(noTaskTimeout, "noTaskTimeout cannot be null");
        this.betweenTaskTimeout = Objects.requireNonNull(betweenTaskTimeout, "betweenTaskTimeout cannot be null");
        this.threadCount = threadCount == null ? 1 : threadCount;
        this.fatalCrashTimeout = fatalCrashTimeout == null ? DEFAULT_TIMEOUT_ON_FATAL_CRASH : fatalCrashTimeout;
        this.retryType = retryType == null ? TaskRetryType.GEOMETRIC_BACKOFF : retryType;
        this.retryInterval = retryInterval == null ? Duration.ofMinutes(1) : retryInterval;
        this.reenqueueRetrySettings = reenqueueRetrySettings == null
                ? ReenqueueRetrySettings.createDefault()
                : reenqueueRetrySettings;
        this.processingMode = processingMode == null ? ProcessingMode.SEPARATE_TRANSACTIONS : processingMode;
        this.additionalSettings = additionalSettings == null ? Collections.emptyMap() :
                Collections.unmodifiableMap(new HashMap<>(additionalSettings));
    }

    /**
     * Get task execution retry strategy.
     *
     * @return Task execution retry strategy.
     */
    @Nonnull
    public TaskRetryType getRetryType() {
        return retryType;
    }

    /**
     * Get retry interval for task execution.
     *
     * @return Task retry interval.
     * @see TaskRetryType
     */
    @Nonnull
    public Duration getRetryInterval() {
        return retryInterval;
    }

    /**
     * Settings for the task postponing strategy
     * when the task should be brought back to the queue.
     *
     * @return Task postponing settings.
     */
    @Nonnull
    public ReenqueueRetrySettings getReenqueueRetrySettings() {
        return reenqueueRetrySettings;
    }

    /**
     * Get number of threads for processing tasks in the queue.
     *
     * @return Number of processing threads.
     */
    public int getThreadCount() {
        return threadCount;
    }

    /**
     * Get delay duration between picking tasks from the queue if there are no task for processing.
     *
     * @return Delay when there are no tasks to process.
     */
    @Nonnull
    public Duration getNoTaskTimeout() {
        return noTaskTimeout;
    }

    /**
     * Get delay duration between picking tasks from the queue after the task was processed.
     *
     * @return Delay after next task was processed.
     */
    @Nonnull
    public Duration getBetweenTaskTimeout() {
        return betweenTaskTimeout;
    }

    /**
     * Get delay duration when task execution thread sleeps after unexpected error.
     *
     * @return Delay after unexpected error.
     */
    @Nonnull
    public Duration getFatalCrashTimeout() {
        return fatalCrashTimeout;
    }

    /**
     * Get task processing mode in the queue.
     *
     * @return Task processing mode.
     */
    @Nonnull
    public ProcessingMode getProcessingMode() {
        return processingMode;
    }

    /**
     * Get all additional properties for the queue.
     *
     * @return Additional queue properties.
     */
    @Nonnull
    public Map<String, String> getAdditionalSettings() {
        return additionalSettings;
    }

    /**
     * Get string value of additional queue property.
     *
     * @param settingName Name of the property.
     * @return Property value.
     */
    @Nonnull
    public String getProperty(@Nonnull String settingName) {
        Objects.requireNonNull(settingName);
        return Objects.requireNonNull(additionalSettings.get(settingName),
                String.format("null values are not allowed: settingName=%s", settingName));
    }

    /**
     * Get {@linkplain Duration} value of additional queue property.
     *
     * @param settingName Name of the property.
     * @return Property value.
     */
    @Nonnull
    public Duration getDurationProperty(@Nonnull String settingName) {
        return DURATION_CONVERTER.apply(getProperty(settingName));
    }

    /**
     * Create a new builder for queue settings.
     *
     * @return A new builder for queue settings.
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return '{' +
                "threadCount=" + threadCount +
                ", betweenTaskTimeout=" + betweenTaskTimeout +
                ", noTaskTimeout=" + noTaskTimeout +
                ", processingMode=" + processingMode +
                ", retryType=" + retryType +
                ", retryInterval=" + retryInterval +
                ", reenqueueRetrySettings=" + reenqueueRetrySettings +
                ", fatalCrashTimeout=" + fatalCrashTimeout +
                (additionalSettings.isEmpty() ? "" : ", additionalSettings=" + additionalSettings) +
                '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        QueueSettings that = (QueueSettings) obj;
        return threadCount == that.threadCount &&
                retryType == that.retryType &&
                processingMode == that.processingMode &&
                Objects.equals(reenqueueRetrySettings, that.reenqueueRetrySettings) &&
                Objects.equals(noTaskTimeout, that.noTaskTimeout) &&
                Objects.equals(betweenTaskTimeout, that.betweenTaskTimeout) &&
                Objects.equals(fatalCrashTimeout, that.fatalCrashTimeout) &&
                Objects.equals(retryInterval, that.retryInterval) &&
                Objects.equals(additionalSettings, that.additionalSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(threadCount, noTaskTimeout, betweenTaskTimeout, fatalCrashTimeout, retryType, reenqueueRetrySettings,
                processingMode, retryInterval, additionalSettings);
    }

    /**
     * A builder for queue settings.
     */
    public static class Builder {
        private Duration noTaskTimeout;
        private Duration betweenTaskTimeout;
        private Duration fatalCrashTimeout;
        private Integer threadCount;
        private TaskRetryType retryType;
        private Duration retryInterval;
        private ReenqueueRetrySettings reenqueueRetrySettings;
        private ProcessingMode processingMode;
        private final Map<String, String> additionalSettings = new HashMap<>();

        private Builder() {
        }

        /**
         * Set delay duration between picking tasks from the queue
         * if there are no task for processing.
         *
         * @param noTaskTimeout Delay when there are no tasks to process.
         * @return Reference to the same builder.
         */
        public Builder withNoTaskTimeout(@Nonnull Duration noTaskTimeout) {
            this.noTaskTimeout = Objects.requireNonNull(noTaskTimeout);
            return this;
        }

        /**
         * Set delay duration between picking tasks from the queue
         * after the task was processed.
         *
         * @param betweenTaskTimeout Delay after next task was processed.
         * @return Reference to the same builder.
         */
        public Builder withBetweenTaskTimeout(@Nonnull Duration betweenTaskTimeout) {
            this.betweenTaskTimeout = Objects.requireNonNull(betweenTaskTimeout);
            return this;
        }

        /**
         * Set delay duration after unexpected error.
         *
         * @param fatalCrashTimeout Delay after unexpected error.
         * @return Reference to the same builder.
         */
        public Builder withFatalCrashTimeout(@Nullable Duration fatalCrashTimeout) {
            this.fatalCrashTimeout = fatalCrashTimeout;
            return this;
        }

        /**
         * Set number of threads for processing tasks in the queue.
         *
         * @param threadCount Number of processing threads.
         * @return Reference to the same builder.
         */
        public Builder withThreadCount(@Nullable Integer threadCount) {
            this.threadCount = threadCount;
            return this;
        }

        /**
         * Set task execution retry strategy.
         *
         * @param taskRetryType Task execution retry strategy.
         * @return Reference to the same builder.
         */
        public Builder withRetryType(@Nullable TaskRetryType taskRetryType) {
            this.retryType = taskRetryType;
            return this;
        }

        /**
         * Set retry interval for task execution.
         *
         * @param retryInterval Task retry interval.
         * @return Reference to the same builder.
         */
        public Builder withRetryInterval(@Nullable Duration retryInterval) {
            this.retryInterval = retryInterval;
            return this;
        }

        /**
         * Set Settings for the task postponing strategy
         * when the task should be brought back to the queue.
         *
         * @param reenqueueRetrySettings Task postponing settings.
         * @return Reference to the same builder.
         */
        public Builder withReenqueueRetrySettings(@Nullable ReenqueueRetrySettings reenqueueRetrySettings) {
            this.reenqueueRetrySettings = reenqueueRetrySettings;
            return this;
        }

        /**
         * Set task processing mode in the queue.
         *
         * @param processingMode Task processing mode.
         * @return Reference to the same builder.
         */
        public Builder withProcessingMode(@Nullable ProcessingMode processingMode) {
            this.processingMode = processingMode;
            return this;
        }

        /**
         * Set the map of additional properties for the queue.
         *
         * @param additionalSettings Additional properties for the queue.
         * @return Reference to the same builder.
         */
        public Builder withAdditionalSettings(@Nonnull Map<String, String> additionalSettings) {
            Objects.requireNonNull(additionalSettings);
            this.additionalSettings.putAll(additionalSettings);
            return this;
        }

        /**
         * Set additional property for the queue.
         *
         * @param settingName Property name.
         * @param value       Property value.
         * @return Reference to the same builder.
         */
        public Builder putSetting(String settingName, String value) {
            additionalSettings.put(settingName, value);
            return this;
        }

        /**
         * Create new queue settings object.
         *
         * @return A new queue settings object.
         */
        public QueueSettings build() {
            return new QueueSettings(noTaskTimeout, betweenTaskTimeout, fatalCrashTimeout, threadCount,
                    retryType, retryInterval, reenqueueRetrySettings, processingMode, additionalSettings);
        }
    }


    private static class Memoizer<T, U> {

        private final Map<T, U> cache = new ConcurrentHashMap<>();

        private Function<T, U> doMemoize(Function<T, U> function) {
            return input -> cache.computeIfAbsent(input, function);
        }

        private static <T, U> Function<T, U> memoize(Function<T, U> function) {
            return new Memoizer<T, U>().doMemoize(function);
        }
    }
}
