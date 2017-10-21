/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Sum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskIdFormatException;
import org.apache.kafka.streams.processor.PartitionGrouper;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static java.util.Collections.singleton;

public class StreamThread extends Thread {

    private static final Logger log = LoggerFactory.getLogger(StreamThread.class);
    private static final AtomicInteger STREAM_THREAD_ID_SEQUENCE = new AtomicInteger(1);

    /**
     * Stream thread states are the possible states that a stream thread can be in.
     * A thread must only be in one state at a time
     * The expected state transitions with the following defined states is:
     * <p>
     * <pre>
     *                +-------------+
     *          +<--- | Created     |
     *          |     +-----+-------+
     *          |           |
     *          |           v
     *          |     +-----+-------+
     *          +<--- | Running     | <----+
     *          |     +-----+-------+      |
     *          |           |              |
     *          |           v              |
     *          |     +-----+-------+      |
     *          +<--- | Partitions  | <-+  |
     *          |     | Revoked     | --+  |
     *          |     +-----+-------+      |
     *          |           |              |
     *          |           v              |
     *          |     +-----+-------+      |
     *          +<--- | Partitions  |+---> |
     *          |     | Assigned    |
     *          |     ------+-------+
     *          |           |
     *          |           v
     *          |     +-----+-------+
     *          +---> | Pending     |
     *          |     | Shutdown    |
     *          |     +-----+-------+
     *          |           |
     *          |           v
     *          |     +-----+-------+
     *          +---> | Dead        |
     *                +-------------+
     * </pre>
     * <p>
     * Note the following:
     * - Any state can go to PENDING_SHUTDOWN. That is because streams can be closed at any time.
     * - Any state can go to DEAD. That is because exceptions can happen at any other state,
     * leading to the stream thread terminating.
     * - A streams thread can stay in PARTITIONS_REVOKED indefinitely, in the corner case when
     * the coordinator repeatedly fails in-between revoking partitions and assigning new partitions.
     */
    public enum State implements ThreadStateTransitionValidator {
        CREATED(1, 4), RUNNING(2, 4), PARTITIONS_REVOKED(2, 3, 4), PARTITIONS_ASSIGNED(1, 2, 4),  PENDING_SHUTDOWN(5), DEAD;

        private final Set<Integer> validTransitions = new HashSet<>();

        State(final Integer... validTransitions) {
            this.validTransitions.addAll(Arrays.asList(validTransitions));
        }

        public boolean isRunning() {
            return !equals(PENDING_SHUTDOWN) && !equals(CREATED) && !equals(DEAD);
        }

        @Override
        public boolean isValidTransition(final ThreadStateTransitionValidator newState) {
            State tmpState = (State) newState;
            return validTransitions.contains(tmpState.ordinal());
        }
    }

    /**
     * Listen to state change events
     */
    public interface StateListener {

        /**
         * Called when state changes
         * @param thread       thread changing state
         * @param newState     current state
         * @param oldState     previous state
         */
        void onChange(final Thread thread, final ThreadStateTransitionValidator newState, final ThreadStateTransitionValidator oldState);
    }

    private class RebalanceListener implements ConsumerRebalanceListener {
        private final Time time;

        RebalanceListener(final Time time) {
            this.time = time;
        }

        @Override
        public void onPartitionsAssigned(final Collection<TopicPartition> assignment) {
            log.debug("{} at state {}: new partitions {} assigned at the end of consumer rebalance.\n" +
                              "\tassigned active tasks: {}\n" +
                              "\tassigned standby tasks: {}\n" +
                              "\tcurrent suspended active tasks: {}\n" +
                              "\tcurrent suspended standby tasks: {}\n",
                      logPrefix,
                      state,
                      assignment,
                      partitionAssignor.activeTasks().keySet(),
                      partitionAssignor.standbyTasks().keySet(),
                      active.previousTaskIds(),
                      standby.previousTaskIds());

            final long start = time.milliseconds();
            try {
                if (!setState(State.PARTITIONS_ASSIGNED)) {
                    return;
                }
                // do this first as we may have suspended standby tasks that
                // will become active or vice versa
                closeNonAssignedSuspendedStandbyTasks();
                closeNonAssignedSuspendedTasks();
                addStreamTasks(assignment);
                addStandbyTasks();
                streamsMetadataState.onChange(partitionAssignor.getPartitionsByHostState(), partitionAssignor.clusterMetadata());
                storeChangelogReader.reset();
                Set<TopicPartition> partitions = active.uninitializedPartitions();
                log.trace("{} pausing partitions: {}", logPrefix, partitions);
                consumer.pause(partitions);
            } catch (final Throwable t) {
                rebalanceException = t;
                throw t;
            } finally {
                log.info("{} partition assignment took {} ms.\n" +
                                 "\tcurrent active tasks: {}\n" +
                                 "\tcurrent standby tasks: {}\n" +
                                 "\tprevious active tasks: {}\n",
                         logPrefix,
                         time.milliseconds() - start,
                         active.allAssignedTaskIds(),
                         standby.allAssignedTaskIds(),
                         active.previousTaskIds());
            }
        }

        @Override
        public void onPartitionsRevoked(final Collection<TopicPartition> assignment) {
            log.debug("{} at state {}: partitions {} revoked at the beginning of consumer rebalance.\n" +
                              "\tcurrent assigned active tasks: {}\n" +
                              "\tcurrent assigned standby tasks: {}\n",
                      logPrefix,
                      state,
                      assignment,
                      active.runningTaskIds(), standby.runningTaskIds());

            final long start = time.milliseconds();
            try {
                setState(State.PARTITIONS_REVOKED);
                // suspend active tasks
                suspendTasksAndState();
            } catch (final Throwable t) {
                rebalanceException = t;
                throw t;
            } finally {
                streamsMetadataState.onChange(Collections.<HostInfo, Set<TopicPartition>>emptyMap(), partitionAssignor.clusterMetadata());
                standbyRecords.clear();

                log.info("{} partition revocation took {} ms.\n" +
                                 "\tsuspended active tasks: {}\n" +
                                 "\tsuspended standby tasks: {}",
                         logPrefix,
                         time.milliseconds() - start,
                         active.suspendedTaskIds(),
                         standby.suspendedTaskIds());
            }
        }
    }

    abstract class AbstractTaskCreator {
        void createTasks(final Map<TaskId, Set<TopicPartition>> tasksToBeCreated) {
            for (final Map.Entry<TaskId, Set<TopicPartition>> newTaskAndPartitions : tasksToBeCreated.entrySet()) {
                final TaskId taskId = newTaskAndPartitions.getKey();
                final Set<TopicPartition> partitions = newTaskAndPartitions.getValue();
                createTask(taskId, partitions);
            }
        }

        abstract void createTask(final TaskId id, final Set<TopicPartition> partitions);
    }

    class TaskCreator extends AbstractTaskCreator {
        @Override
        void createTask(final TaskId taskId, final Set<TopicPartition> partitions) {
            active.addNewTask(createStreamTask(taskId, partitions));
        }
    }

    class StandbyTaskCreator extends AbstractTaskCreator {

        @Override
        void createTask(final TaskId taskId, final Set<TopicPartition> partitions) {
            final StandbyTask task = createStandbyTask(taskId, partitions);
            if (task != null) {
                standby.addNewTask(task);
            }
        }
    }

    /**
     * This class extends {@link StreamsMetricsImpl(Metrics, String, String, Map)} and
     * overrides one of its functions for efficiency
     */
    private class StreamsMetricsThreadImpl extends StreamsMetricsImpl {
        final Sensor commitTimeSensor;
        final Sensor pollTimeSensor;
        final Sensor processTimeSensor;
        final Sensor punctuateTimeSensor;
        final Sensor taskCreatedSensor;
        final Sensor tasksClosedSensor;
        final Sensor skippedRecordsSensor;

        StreamsMetricsThreadImpl(final Metrics metrics, final String groupName, final String prefix, final Map<String, String> tags) {
            super(metrics, groupName, tags);
            commitTimeSensor = metrics.sensor(prefix + ".commit-latency", Sensor.RecordingLevel.INFO);
            commitTimeSensor.add(metrics.metricName("commit-latency-avg", this.groupName, "The average commit time in ms", this.tags), new Avg());
            commitTimeSensor.add(metrics.metricName("commit-latency-max", this.groupName, "The maximum commit time in ms", this.tags), new Max());
            commitTimeSensor.add(metrics.metricName("commit-rate", this.groupName, "The average per-second number of commit calls", this.tags), new Rate(new Count()));

            pollTimeSensor = metrics.sensor(prefix + ".poll-latency", Sensor.RecordingLevel.INFO);
            pollTimeSensor.add(metrics.metricName("poll-latency-avg", this.groupName, "The average poll time in ms", this.tags), new Avg());
            pollTimeSensor.add(metrics.metricName("poll-latency-max", this.groupName, "The maximum poll time in ms", this.tags), new Max());
            pollTimeSensor.add(metrics.metricName("poll-rate", this.groupName, "The average per-second number of record-poll calls", this.tags), new Rate(new Count()));

            processTimeSensor = metrics.sensor(prefix + ".process-latency", Sensor.RecordingLevel.INFO);
            processTimeSensor.add(metrics.metricName("process-latency-avg", this.groupName, "The average process time in ms", this.tags), new Avg());
            processTimeSensor.add(metrics.metricName("process-latency-max", this.groupName, "The maximum process time in ms", this.tags), new Max());
            processTimeSensor.add(metrics.metricName("process-rate", this.groupName, "The average per-second number of process calls", this.tags), new Rate(new Count()));

            punctuateTimeSensor = metrics.sensor(prefix + ".punctuate-latency", Sensor.RecordingLevel.INFO);
            punctuateTimeSensor.add(metrics.metricName("punctuate-latency-avg", this.groupName, "The average punctuate time in ms", this.tags), new Avg());
            punctuateTimeSensor.add(metrics.metricName("punctuate-latency-max", this.groupName, "The maximum punctuate time in ms", this.tags), new Max());
            punctuateTimeSensor.add(metrics.metricName("punctuate-rate", this.groupName, "The average per-second number of punctuate calls", this.tags), new Rate(new Count()));

            taskCreatedSensor = metrics.sensor(prefix + ".task-created", Sensor.RecordingLevel.INFO);
            taskCreatedSensor.add(metrics.metricName("task-created-rate", this.groupName, "The average per-second number of newly created tasks", this.tags), new Rate(new Count()));

            tasksClosedSensor = metrics.sensor(prefix + ".task-closed", Sensor.RecordingLevel.INFO);
            tasksClosedSensor.add(metrics.metricName("task-closed-rate", this.groupName, "The average per-second number of closed tasks", this.tags), new Rate(new Count()));

            skippedRecordsSensor = metrics.sensor(prefix + ".skipped-records");
            skippedRecordsSensor.add(metrics.metricName("skipped-records-rate", this.groupName, "The average per-second number of skipped records.", this.tags), new Rate(new Sum()));

        }


        @Override
        public void recordLatency(final Sensor sensor, final long startNs, final long endNs) {
            sensor.record(endNs - startNs, timerStartedMs);
        }

        void removeAllSensors() {
            removeSensor(commitTimeSensor);
            removeSensor(pollTimeSensor);
            removeSensor(processTimeSensor);
            removeSensor(punctuateTimeSensor);
            removeSensor(taskCreatedSensor);
            removeSensor(tasksClosedSensor);
            removeSensor(skippedRecordsSensor);

        }
    }


    private volatile State state = State.CREATED;
    private final Object stateLock = new Object();
    private StreamThread.StateListener stateListener = null;
    final PartitionGrouper partitionGrouper;
    private final StreamsMetadataState streamsMetadataState;
    public final String applicationId;
    public final String clientId;
    public final UUID processId;

    protected final StreamsConfig config;
    protected final TopologyBuilder builder;
    Producer<byte[], byte[]> threadProducer;
    private final KafkaClientSupplier clientSupplier;
    protected final Consumer<byte[], byte[]> consumer;
    final Consumer<byte[], byte[]> restoreConsumer;

    private final String logPrefix;
    private final String threadClientId;
    private final Pattern sourceTopicPattern;
    private final AssignedTasks<StreamTask> active;
    private final AssignedTasks<StandbyTask> standby;

    private final Time time;
    private final long pollTimeMs;
    private final long commitTimeMs;
    private final StreamsMetricsThreadImpl streamsMetrics;
    // TODO: this is not private only for tests, should be better refactored
    final StateDirectory stateDirectory;
    private String originalReset;
    private StreamPartitionAssignor partitionAssignor;
    private long timerStartedMs;
    private long lastCommitMs;
    private Throwable rebalanceException = null;
    private final boolean eosEnabled;

    private Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> standbyRecords;
    private boolean processStandbyRecords = false;

    private final ThreadCache cache;
    final StoreChangelogReader storeChangelogReader;

    private final TaskCreator taskCreator = new TaskCreator();

    final ConsumerRebalanceListener rebalanceListener;
    private final static int UNLIMITED_RECORDS = -1;

    public StreamThread(final TopologyBuilder builder,
                        final StreamsConfig config,
                        final KafkaClientSupplier clientSupplier,
                        final String applicationId,
                        final String clientId,
                        final UUID processId,
                        final Metrics metrics,
                        final Time time,
                        final StreamsMetadataState streamsMetadataState,
                        final long cacheSizeBytes,
                        final StateDirectory stateDirectory) {
        super(clientId + "-StreamThread-" + STREAM_THREAD_ID_SEQUENCE.getAndIncrement());
        this.applicationId = applicationId;
        this.config = config;
        this.builder = builder;
        this.clientSupplier = clientSupplier;
        sourceTopicPattern = builder.sourceTopicPattern();
        this.clientId = clientId;
        this.processId = processId;
        partitionGrouper = config.getConfiguredInstance(StreamsConfig.PARTITION_GROUPER_CLASS_CONFIG, PartitionGrouper.class);
        this.streamsMetadataState = streamsMetadataState;
        threadClientId = getName();
        logPrefix = String.format("stream-thread [%s]", threadClientId);

        streamsMetrics = new StreamsMetricsThreadImpl(metrics, "stream-metrics", "thread." + threadClientId,
            Collections.singletonMap("client-id", threadClientId));
        if (config.getLong(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG) < 0) {
            log.warn("{} Negative cache size passed in thread. Reverting to cache size of 0 bytes", logPrefix);
        }
        cache = new ThreadCache(threadClientId, cacheSizeBytes, streamsMetrics);
        eosEnabled = StreamsConfig.EXACTLY_ONCE.equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG));


        // set the consumer clients
        log.info("{} Creating consumer client", logPrefix);
        final Map<String, Object> consumerConfigs = config.getConsumerConfigs(this, applicationId, threadClientId);

        if (!builder.latestResetTopicsPattern().pattern().equals("") || !builder.earliestResetTopicsPattern().pattern().equals("")) {
            originalReset = (String) consumerConfigs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
            log.info("{} Custom offset resets specified updating configs original auto offset reset {}", logPrefix, originalReset);
            consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        }

        consumer = clientSupplier.getConsumer(consumerConfigs);
        log.info("{} Creating restore consumer client", logPrefix);
        restoreConsumer = clientSupplier.getRestoreConsumer(config.getRestoreConsumerConfigs(threadClientId));
        // standby KTables
        standbyRecords = new HashMap<>();


        this.stateDirectory = stateDirectory;
        pollTimeMs = config.getLong(StreamsConfig.POLL_MS_CONFIG);
        commitTimeMs = config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG);

        this.time = time;
        timerStartedMs = time.milliseconds();
        lastCommitMs = timerStartedMs;
        final Integer requestTimeOut = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        rebalanceListener = new RebalanceListener(time);
        active = new AssignedTasks<>(logPrefix, "stream task", Time.SYSTEM);
        standby = new AssignedTasks<>(logPrefix, "standby task", Time.SYSTEM);
        storeChangelogReader = new StoreChangelogReader(getName(), restoreConsumer, time, requestTimeOut);
    }

    /**
     * Execute the stream processors
     *
     * @throws KafkaException for any Kafka-related exceptions
     * @throws Exception      for any other non-Kafka exceptions
     */
    @Override
    public void run() {
        log.info("{} Starting", logPrefix);
        setState(State.RUNNING);
        boolean cleanRun = false;
        try {
            runLoop();
            cleanRun = true;
        } catch (final KafkaException e) {
            // just re-throw the exception as it should be logged already
            throw e;
        } catch (final Exception e) {
            // we have caught all Kafka related exceptions, and other runtime exceptions
            // should be due to user application errors
            log.error("{} Encountered the following error during processing:", logPrefix, e);
            throw e;
        } finally {
            shutdown(cleanRun);
        }
    }

    /**
     * Main event loop for polling, and processing records through topologies.
     */
    private void runLoop() {
        long recordsProcessedBeforeCommit = UNLIMITED_RECORDS;
        consumer.subscribe(sourceTopicPattern, rebalanceListener);

        while (stillRunning()) {
            recordsProcessedBeforeCommit = runOnce(recordsProcessedBeforeCommit);
        }
        log.info("{} Shutting down at user request", logPrefix);
    }

    // Visible for testing
    long runOnce(long recordsProcessedBeforeCommit) {
        timerStartedMs = time.milliseconds();

        // try to fetch some records if necessary
        final ConsumerRecords<byte[], byte[]> records = pollRequests();

        if (state == State.PARTITIONS_ASSIGNED) {
            active.initializeNewTasks();
            standby.initializeNewTasks();

            final Collection<TopicPartition> restored = storeChangelogReader.restore();
            final Set<TopicPartition> resumed = active.updateRestored(restored);

            if (!resumed.isEmpty()) {
                log.trace("{} resuming partitions {}", logPrefix, resumed);
                consumer.resume(resumed);
            }

            if (active.allTasksRunning()) {
                assignStandbyPartitions();
                setState(State.RUNNING);
            }
        }

        if (records != null && !records.isEmpty() && active.hasRunningTasks()) {
            streamsMetrics.pollTimeSensor.record(computeLatency(), timerStartedMs);
            addRecordsToTasks(records);
            final long totalProcessed = processAndPunctuate(recordsProcessedBeforeCommit);
            if (totalProcessed > 0) {
                final long processLatency = computeLatency();
                streamsMetrics.processTimeSensor.record(processLatency / (double) totalProcessed,
                                                        timerStartedMs);
                recordsProcessedBeforeCommit = adjustRecordsProcessedBeforeCommit(recordsProcessedBeforeCommit, totalProcessed,
                                                                                  processLatency, commitTimeMs);
            }
        }

        maybeCommit(timerStartedMs);
        maybeUpdateStandbyTasks(timerStartedMs);
        return recordsProcessedBeforeCommit;
    }

    /**
     * Get the next batch of records by polling.
     * @return Next batch of records or null if no records available.
     */
    private ConsumerRecords<byte[], byte[]> pollRequests() {
        ConsumerRecords<byte[], byte[]> records = null;

        try {
            records = consumer.poll(pollTimeMs);
        } catch (final InvalidOffsetException e) {
            resetInvalidOffsets(e);
        }

        if (rebalanceException != null) {
            if (!(rebalanceException instanceof ProducerFencedException)) {
                throw new StreamsException(logPrefix + " Failed to rebalance.", rebalanceException);
            }
        }

        return records;
    }

    private void resetInvalidOffsets(final InvalidOffsetException e) {
        final Set<TopicPartition> partitions = e.partitions();
        final Set<String> loggedTopics = new HashSet<>();
        final Set<TopicPartition> seekToBeginning = new HashSet<>();
        final Set<TopicPartition> seekToEnd = new HashSet<>();

        for (final TopicPartition partition : partitions) {
            if (builder.earliestResetTopicsPattern().matcher(partition.topic()).matches()) {
                addToResetList(partition, seekToBeginning, "{} Setting topic '{}' to consume from {} offset", "earliest", loggedTopics);
            } else if (builder.latestResetTopicsPattern().matcher(partition.topic()).matches()) {
                addToResetList(partition, seekToEnd, "{} Setting topic '{}' to consume from {} offset", "latest", loggedTopics);
            } else {
                if (originalReset == null || (!originalReset.equals("earliest") && !originalReset.equals("latest"))) {
                    setState(State.PENDING_SHUTDOWN);
                    final String errorMessage = "No valid committed offset found for input topic %s (partition %s) and no valid reset policy configured." +
                        " You need to set configuration parameter \"auto.offset.reset\" or specify a topic specific reset " +
                        "policy via KStreamBuilder#stream(StreamsConfig.AutoOffsetReset offsetReset, ...) or KStreamBuilder#table(StreamsConfig.AutoOffsetReset offsetReset, ...)";
                    throw new StreamsException(String.format(errorMessage, partition.topic(), partition.partition()), e);
                }

                if (originalReset.equals("earliest")) {
                    addToResetList(partition, seekToBeginning, "{} No custom setting defined for topic '{}' using original config '{}' for offset reset", "earliest", loggedTopics);
                } else if (originalReset.equals("latest")) {
                    addToResetList(partition, seekToEnd, "{} No custom setting defined for topic '{}' using original config '{}' for offset reset", "latest", loggedTopics);
                }
            }
        }

        if (!seekToBeginning.isEmpty()) {
            consumer.seekToBeginning(seekToBeginning);
        }
        if (!seekToEnd.isEmpty()) {
            consumer.seekToEnd(seekToEnd);
        }
    }

    private void addToResetList(final TopicPartition partition, final Set<TopicPartition> partitions, final String logMessage, final String resetPolicy, final Set<String> loggedTopics) {
        final String topic = partition.topic();
        if (loggedTopics.add(topic)) {
            log.info(logMessage, logPrefix, topic, resetPolicy);
        }
        partitions.add(partition);
    }

    /**
     * Take records and add them to each respective task
     * @param records Records, can be null
     */
    private void addRecordsToTasks(final ConsumerRecords<byte[], byte[]> records) {
        if (records != null && !records.isEmpty()) {
            int numAddedRecords = 0;

            for (final TopicPartition partition : records.partitions()) {
                final StreamTask task = active.runningTaskFor(partition);
                numAddedRecords += task.addRecords(partition, records.records(partition));
            }
            streamsMetrics.skippedRecordsSensor.record(records.count() - numAddedRecords, timerStartedMs);
        }
    }

    /**
     * Schedule the records processing by selecting which record is processed next. Commits may
     * happen as records are processed.
     * @param recordsProcessedBeforeCommit number of records to be processed before commit is called.
     *                                     if UNLIMITED_RECORDS, then commit is never called
     * @return Number of records processed since last commit.
     */
    private long processAndPunctuate(final long recordsProcessedBeforeCommit) {

        int processed;
        long totalProcessedSinceLastMaybeCommit = 0;
        // Round-robin scheduling by taking one record from each task repeatedly
        // until no task has any records left
        do {
            processed = active.process();
            totalProcessedSinceLastMaybeCommit += processed;

            if (recordsProcessedBeforeCommit != UNLIMITED_RECORDS &&
                totalProcessedSinceLastMaybeCommit >= recordsProcessedBeforeCommit) {
                totalProcessedSinceLastMaybeCommit = 0;
                final long processLatency = computeLatency();
                streamsMetrics.processTimeSensor.record(processLatency / (double) totalProcessedSinceLastMaybeCommit,
                    timerStartedMs);
                maybeCommit(timerStartedMs);
            }
        } while (processed != 0);

        // go over the tasks again to punctuate or commit
        active.punctuateAndCommit(streamsMetrics.commitTimeSensor, streamsMetrics.punctuateTimeSensor);
        return totalProcessedSinceLastMaybeCommit;
    }

    /**
     * Adjust the number of records that should be processed by scheduler. This avoids
     * scenarios where the processing time is higher than the commit time.
     * @param prevRecordsProcessedBeforeCommit Previous number of records processed by scheduler.
     * @param totalProcessed Total number of records processed in this last round.
     * @param processLatency Total processing latency in ms processed in this last round.
     * @param commitTime Desired commit time in ms.
     * @return An adjusted number of records to be processed in the next round.
     */
    private long adjustRecordsProcessedBeforeCommit(final long prevRecordsProcessedBeforeCommit, final long totalProcessed,
                                                    final long processLatency, final long commitTime) {
        long recordsProcessedBeforeCommit = UNLIMITED_RECORDS;
        // check if process latency larger than commit latency
        // note that once we set recordsProcessedBeforeCommit, it will never be UNLIMITED_RECORDS again, so
        // we will never process all records again. This might be an issue if the initial measurement
        // was off due to a slow start.
        if (processLatency > 0 && processLatency > commitTime) {
            // push down
            recordsProcessedBeforeCommit = Math.max(1, (commitTime * totalProcessed) / processLatency);
            log.debug("{} processing latency {} > commit time {} for {} records. Adjusting down recordsProcessedBeforeCommit={}",
                logPrefix, processLatency, commitTime, totalProcessed, recordsProcessedBeforeCommit);
        } else if (prevRecordsProcessedBeforeCommit != UNLIMITED_RECORDS && processLatency > 0) {
            // push up
            recordsProcessedBeforeCommit = Math.max(1, (commitTime * totalProcessed) / processLatency);
            log.debug("{} processing latency {} < commit time {} for {} records. Adjusting up recordsProcessedBeforeCommit={}",
                logPrefix, processLatency, commitTime, totalProcessed, recordsProcessedBeforeCommit);
        }

        return recordsProcessedBeforeCommit;
    }

    /**
     * Commit all tasks owned by this thread if specified interval time has elapsed
     */
    protected void maybeCommit(final long now) {
        if (commitTimeMs >= 0 && lastCommitMs + commitTimeMs < now) {
            if (log.isTraceEnabled()) {
                log.trace("{} Committing all active tasks {} and standby tasks {} since {}ms has elapsed (commit interval is {}ms)",
                          logPrefix, active.runningTaskIds(), standby.runningTaskIds(), now - lastCommitMs, commitTimeMs);
            }

            commitAll();

            if (log.isDebugEnabled()) {
                log.info("{} Committed all active tasks {} and standby tasks {} in {}ms",
                         logPrefix, active.runningTaskIds(), standby.runningTaskIds(), timerStartedMs - now);
            }

            lastCommitMs = now;

            processStandbyRecords = true;
        }
    }

    /**
     * Commit the states of all its tasks
     */
    private void commitAll() {
        active.commit();
        standby.commit();
    }

    private void assignStandbyPartitions() {
        final Collection<StandbyTask> running = standby.runningTasks();
        final Map<TopicPartition, Long> checkpointedOffsets = new HashMap<>();
        for (StandbyTask standbyTask : running) {
            checkpointedOffsets.putAll(standbyTask.checkpointedOffsets());
        }

        final List<TopicPartition> assignment = new ArrayList<>(checkpointedOffsets.keySet());
        restoreConsumer.assign(assignment);
        for (final Map.Entry<TopicPartition, Long> entry : checkpointedOffsets.entrySet()) {
            final TopicPartition partition = entry.getKey();
            final long offset = entry.getValue();
            if (offset >= 0) {
                restoreConsumer.seek(partition, offset);
            } else {
                restoreConsumer.seekToBeginning(singleton(partition));
            }
        }
        log.trace("{} assigned {} partitions to restore consumer for standby tasks {}", logPrefix, assignment, standby.runningTaskIds());
    }

    private void maybeUpdateStandbyTasks(final long now) {
        if (state == State.RUNNING && standby.hasRunningTasks()) {
            if (processStandbyRecords) {
                if (!standbyRecords.isEmpty()) {
                    final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> remainingStandbyRecords = new HashMap<>();

                    for (final Map.Entry<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> entry : standbyRecords.entrySet()) {
                        final TopicPartition partition = entry.getKey();
                        List<ConsumerRecord<byte[], byte[]>> remaining = entry.getValue();
                        if (remaining != null) {
                            final StandbyTask task = standby.runningTaskFor(partition);
                            remaining = task.update(partition, remaining);
                            if (remaining != null) {
                                remainingStandbyRecords.put(partition, remaining);
                            } else {
                                restoreConsumer.resume(singleton(partition));
                            }
                        }
                    }

                    standbyRecords = remainingStandbyRecords;

                    log.debug("{} Updated standby tasks {} in {}ms", logPrefix, standby.runningTaskIds(), time.milliseconds() - now);
                }
                processStandbyRecords = false;
            }

            final ConsumerRecords<byte[], byte[]> records = restoreConsumer.poll(0);

            if (!records.isEmpty()) {
                for (final TopicPartition partition : records.partitions()) {
                    final StandbyTask task = standby.runningTaskFor(partition);

                    if (task == null) {
                        throw new StreamsException(logPrefix + " Missing standby task for partition " + partition);
                    }

                    final List<ConsumerRecord<byte[], byte[]>> remaining = task.update(partition, records.records(partition));
                    if (remaining != null) {
                        restoreConsumer.pause(singleton(partition));
                        standbyRecords.put(partition, remaining);
                    }
                }
            }
        }
    }

    /**
     * Compute the latency based on the current marked timestamp, and update the marked timestamp
     * with the current system timestamp.
     *
     * @return latency
     */
    private long computeLatency() {
        final long previousTimeMs = timerStartedMs;
        timerStartedMs = time.milliseconds();

        return Math.max(timerStartedMs - previousTimeMs, 0);
    }

    /**
     * Shutdown this stream thread.
     * Note that there is nothing to prevent this function from being called multiple times
     * (e.g., in testing), hence the state is set only the first time
     */
    public synchronized void close() {
        log.info("{} Informed thread to shut down", logPrefix);
        setState(State.PENDING_SHUTDOWN);
    }

    public boolean isInitialized() {
        return state == State.RUNNING;
    }

    public boolean stillRunning() {
        return state.isRunning();
    }

    public Map<TaskId, StreamTask> tasks() {
        return active.runningTaskMap();
    }

    /**
     * Returns ids of tasks that were being executed before the rebalance.
     */
    public Set<TaskId> prevActiveTasks() {
        return Collections.unmodifiableSet(active.previousTaskIds());
    }

    /**
     * Returns ids of tasks whose states are kept on the local storage.
     */
    public Set<TaskId> cachedTasks() {
        // A client could contain some inactive tasks whose states are still kept on the local storage in the following scenarios:
        // 1) the client is actively maintaining standby tasks by maintaining their states from the change log.
        // 2) the client has just got some tasks migrated out of itself to other clients while these task states
        //    have not been cleaned up yet (this can happen in a rolling bounce upgrade, for example).

        final HashSet<TaskId> tasks = new HashSet<>();

        final File[] stateDirs = stateDirectory.listTaskDirectories();
        if (stateDirs != null) {
            for (final File dir : stateDirs) {
                try {
                    final TaskId id = TaskId.parse(dir.getName());
                    // if the checkpoint file exists, the state is valid.
                    if (new File(dir, ProcessorStateManager.CHECKPOINT_FILE_NAME).exists()) {
                        tasks.add(id);
                    }
                } catch (final TaskIdFormatException e) {
                    // there may be some unknown files that sits in the same directory,
                    // we should ignore these files instead trying to delete them as well
                }
            }
        }

        return tasks;
    }

    /**
     * Set the {@link StreamThread.StateListener} to be notified when state changes. Note this API is internal to
     * Kafka Streams and is not intended to be used by an external application.
     */
    public void setStateListener(final StreamThread.StateListener listener) {
        stateListener = listener;
    }

    /**
     * @return The state this instance is in
     */
    public State state() {
        return state;
    }



    /**
     * Sets the state
     * @param newState New state
     */
    boolean setState(final State newState) {
        State oldState;
        synchronized (stateLock) {
            oldState = state;

            // there are cases when we shouldn't check if a transition is valid, e.g.,
            // when, for testing, a thread is closed multiple times. We could either
            // check here and immediately return for those cases, or add them to the transition
            // diagram (but then the diagram would be confusing and have transitions like
            // PENDING_SHUTDOWN->PENDING_SHUTDOWN). These cases include:
            // - normal close() sequence. State is set to PENDING_SHUTDOWN in close() as well as in shutdown().
            // - calling close() on the thread after an exception within the thread has already called shutdown().

            // note we could be going from PENDING_SHUTDOWN to DEAD, and we obviously want to allow that
            // transition, hence the check newState != DEAD.
            if (newState != State.DEAD &&
                    (state == State.PENDING_SHUTDOWN || state == State.DEAD)) {
                return false;
            }
            if (!state.isValidTransition(newState)) {
                log.warn("{} Unexpected state transition from {} to {}.", logPrefix, oldState, newState);
                throw new StreamsException(logPrefix + " Unexpected state transition from " + oldState + " to " + newState);
            } else {
                log.info("{} State transition from {} to {}.", logPrefix, oldState, newState);
            }

            state = newState;
        }
        if (stateListener != null) {
            stateListener.onChange(this, state, oldState);
        }
        return true;
    }

    /**
     * Produces a string representation containing useful information about a StreamThread.
     * This is useful in debugging scenarios.
     * @return A string representation of the StreamThread instance.
     */
    @Override
    public String toString() {
        return toString("");
    }

    /**
     * Produces a string representation containing useful information about a StreamThread, starting with the given indent.
     * This is useful in debugging scenarios.
     * @return A string representation of the StreamThread instance.
     */
    public String toString(final String indent) {
        final StringBuilder sb = new StringBuilder()
            .append(indent).append("StreamsThread appId: ").append(applicationId).append("\n")
            .append(indent).append("\tStreamsThread clientId: ").append(clientId).append("\n")
            .append(indent).append("\tStreamsThread threadId: ").append(getName()).append("\n");

        sb.append(indent).append("\tActive tasks:\n");
        sb.append(active.toString(indent + "\t\t"));
        sb.append(indent).append("\tStandby tasks:\n");
        sb.append(standby.toString(indent + "\t\t"));
        sb.append("\n");

        return sb.toString();
    }

    String threadClientId() {
        return threadClientId;
    }

    void setPartitionAssignor(final StreamPartitionAssignor partitionAssignor) {
        this.partitionAssignor = partitionAssignor;
    }

    // Visible for testing
    void shutdown(final boolean cleanRun) {
        log.info("{} Shutting down", logPrefix);
        setState(State.PENDING_SHUTDOWN);
        shutdownTasksAndState(cleanRun);

        // close all embedded clients
        if (threadProducer != null) {
            try {
                threadProducer.close();
            } catch (final Throwable e) {
                log.error("{} Failed to close producer due to the following error:", logPrefix, e);
            }
        }
        try {
            consumer.close();
        } catch (final Throwable e) {
            log.error("{} Failed to close consumer due to the following error:", logPrefix, e);
        }
        try {
            restoreConsumer.close();
        } catch (final Throwable e) {
            log.error("{} Failed to close restore consumer due to the following error:", logPrefix, e);
        }
        try {
            partitionAssignor.close();
        } catch (final Throwable e) {
            log.error("{} Failed to close KafkaStreamClient due to the following error:", logPrefix, e);
        }

        active.clear();
        standby.clear();

        // clean up global tasks

        log.info("{} Stream thread shutdown complete", logPrefix);
        setState(State.DEAD);
        streamsMetrics.removeAllSensors();
    }

    @SuppressWarnings("ThrowableNotThrown")
    private void shutdownTasksAndState(final boolean cleanRun) {
        log.debug("{} Shutting down all active tasks {}, standby tasks {}, suspended tasks {}, and suspended standby tasks {}",
                  logPrefix, active.runningTaskIds(), standby.runningTaskIds(),
                  active.previousTaskIds(), standby.previousTaskIds());

        for (final AbstractTask task : allTasks()) {
            try {
                task.close(cleanRun, false);
            } catch (final RuntimeException e) {
                log.error("{} Failed while closing {} {} due to the following error:",
                          logPrefix,
                          task.getClass().getSimpleName(),
                          task.id(),
                          e);
            }
        }

        // remove the changelog partitions from restore consumer
        unAssignChangeLogPartitions();
    }

    /**
     * Similar to shutdownTasksAndState, however does not close the task managers, in the hope that
     * soon the tasks will be assigned again
     */
    private void suspendTasksAndState()  {
        log.debug("{} Suspending all active tasks {} and standby tasks {}",
                  logPrefix, active.runningTaskIds(), standby.runningTaskIds());

        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);

        firstException.compareAndSet(null, active.suspend());
        firstException.compareAndSet(null, standby.suspend());
        // remove the changelog partitions from restore consumer
        firstException.compareAndSet(null, unAssignChangeLogPartitions());

        if (firstException.get() != null) {
            throw new StreamsException(logPrefix + " failed to suspend stream tasks", firstException.get());
        }
    }

    private RuntimeException unAssignChangeLogPartitions() {
        try {
            // un-assign the change log partitions
            restoreConsumer.assign(Collections.<TopicPartition>emptyList());
        } catch (final RuntimeException e) {
            log.error("{} Failed to un-assign change log partitions due to the following error:", logPrefix, e);
            return e;
        }
        return null;
    }

    private List<AbstractTask> allTasks() {
        final List<AbstractTask> tasks = active.allInitializedTasks();
        tasks.addAll(standby.allInitializedTasks());
        return tasks;
    }

    private void closeNonAssignedSuspendedTasks() {
        final Map<TaskId, Set<TopicPartition>> newTaskAssignment = partitionAssignor.activeTasks();
        final Iterator<StreamTask> suspendedTaskIterator = active.suspendedTasks().iterator();
        while (suspendedTaskIterator.hasNext()) {
            final StreamTask task = suspendedTaskIterator.next();
            final Set<TopicPartition> assignedPartitionsForTask = newTaskAssignment.get(task.id);
            if (!task.partitions().equals(assignedPartitionsForTask)) {
                log.debug("{} Closing suspended and not re-assigned task {}", logPrefix, task.id());
                try {
                    task.closeSuspended(true, false, null);
                } catch (final Exception e) {
                    log.error("{} Failed to close suspended task {} due to the following error:", logPrefix, task.id, e);
                } finally {
                    suspendedTaskIterator.remove();
                }
            }
        }
    }

    private void closeNonAssignedSuspendedStandbyTasks() {
        final Set<TaskId> newStandbyTaskIds = partitionAssignor.standbyTasks().keySet();
        final Iterator<StandbyTask> standByTaskIterator = standby.suspendedTasks().iterator();
        while (standByTaskIterator.hasNext()) {
            final StandbyTask task = standByTaskIterator.next();
            if (!newStandbyTaskIds.contains(task.id)) {
                log.debug("{} Closing suspended and not re-assigned standby task {}", logPrefix, task.id());
                try {
                    task.close(true, false);
                } catch (final Exception e) {
                    log.error("{} Failed to remove suspended standby task {} due to the following error:", logPrefix, task.id(), e);
                } finally {
                    standByTaskIterator.remove();
                }
            }
        }
    }

    // visible for testing
    protected StreamTask createStreamTask(final TaskId id, final Collection<TopicPartition> partitions) {
        streamsMetrics.taskCreatedSensor.record();

        try {
            return new StreamTask(
                    id,
                    applicationId,
                    partitions,
                    builder.build(id.topicGroupId),
                    consumer,
                    storeChangelogReader,
                    config,
                    streamsMetrics,
                    stateDirectory,
                    cache,
                    time,
                    createProducer(id));
        } finally {
            log.trace("{} Created active task {} with assigned partitions {}", logPrefix, id, partitions);
        }
    }

    private Producer<byte[], byte[]> createProducer(final TaskId id) {

        final Producer<byte[], byte[]> producer;
        if (eosEnabled) {
            final Map<String, Object> producerConfigs = config.getProducerConfigs(threadClientId + "-" + id);
            log.info("{} Creating producer client for task {}", logPrefix, id);
            producerConfigs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, applicationId + "-" + id);
            producer = clientSupplier.getProducer(producerConfigs);
        } else {
            if (threadProducer == null) {
                final Map<String, Object> producerConfigs = config.getProducerConfigs(threadClientId);
                log.info("{} Creating shared producer client", logPrefix);
                threadProducer = clientSupplier.getProducer(producerConfigs);
            }
            producer = threadProducer;
        }

        return producer;
    }

    private void addStreamTasks(final Collection<TopicPartition> assignment) {
        if (partitionAssignor == null) {
            throw new IllegalStateException(logPrefix + " Partition assignor has not been initialized while adding stream tasks: this should not happen.");
        }

        final Map<TaskId, Set<TopicPartition>> newTasks = new HashMap<>();

        // collect newly assigned tasks and reopen re-assigned tasks
        log.debug("{} Adding assigned tasks as active: {}", logPrefix, partitionAssignor.activeTasks());
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : partitionAssignor.activeTasks().entrySet()) {
            final TaskId taskId = entry.getKey();
            final Set<TopicPartition> partitions = entry.getValue();

            if (assignment.containsAll(partitions)) {
                try {
                    if (!active.maybeResumeSuspendedTask(taskId, partitions)) {
                        newTasks.put(taskId, partitions);
                    }
                } catch (final StreamsException e) {
                    log.error("{} Failed to create an active task {} due to the following error:", logPrefix, taskId, e);
                    throw e;
                }
            } else {
                log.warn("{} Task {} owned partitions {} are not contained in the assignment {}", logPrefix, taskId, partitions, assignment);
            }
        }

        // create all newly assigned tasks (guard against race condition with other thread via backoff and retry)
        // -> other thread will call removeSuspendedTasks(); eventually
        log.trace("{} New active tasks to be created: {}", logPrefix, newTasks);

        taskCreator.createTasks(newTasks);
    }

    // visible for testing
    protected StandbyTask createStandbyTask(final TaskId id, final Collection<TopicPartition> partitions) {
        streamsMetrics.taskCreatedSensor.record();

        final ProcessorTopology topology = builder.build(id.topicGroupId);

        if (!topology.stateStores().isEmpty()) {
            try {
                return new StandbyTask(id, applicationId, partitions, topology, consumer, storeChangelogReader, config, streamsMetrics, stateDirectory);
            } finally {
                log.trace("{} Created standby task {} with assigned partitions {}", logPrefix, id, partitions);
            }
        } else {
            log.trace("{} Skipped standby task {} with assigned partitions {} since it does not have any state stores to materialize", logPrefix, id, partitions);

            return null;
        }
    }

    private void addStandbyTasks() {
        if (partitionAssignor == null) {
            throw new IllegalStateException(logPrefix + " Partition assignor has not been initialized while adding standby tasks: this should not happen.");
        }

        final Map<TaskId, Set<TopicPartition>> newStandbyTasks = new HashMap<>();

        log.debug("{} Adding assigned standby tasks {}", logPrefix, partitionAssignor.standbyTasks());
        // collect newly assigned standby tasks and reopen re-assigned standby tasks
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : partitionAssignor.standbyTasks().entrySet()) {
            final TaskId taskId = entry.getKey();
            final Set<TopicPartition> partitions = entry.getValue();
            if (!standby.maybeResumeSuspendedTask(taskId, partitions)) {
                newStandbyTasks.put(taskId, partitions);
            }

        }

        // create all newly assigned standby tasks (guard against race condition with other thread via backoff and retry)
        // -> other thread will call removeSuspendedStandbyTasks(); eventually
        log.trace("{} New standby tasks to be created: {}", logPrefix, newStandbyTasks);

        new StandbyTaskCreator().createTasks(newStandbyTasks);
    }


}
