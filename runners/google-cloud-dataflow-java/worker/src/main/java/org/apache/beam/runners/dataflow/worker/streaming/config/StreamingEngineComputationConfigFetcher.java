/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.dataflow.worker.streaming.config;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

import com.google.api.services.dataflow.model.MapTask;
import com.google.api.services.dataflow.model.StreamingComputationConfig;
import com.google.api.services.dataflow.model.StreamingConfigTask;
import com.google.api.services.dataflow.model.WorkItem;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.StreamSupport;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.WorkUnitClient;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming engine implementation of {@link ComputationConfig.Fetcher}. Asynchronously fetches
 * global pipeline config.
 */
@Internal
@ThreadSafe
public final class StreamingEngineComputationConfigFetcher implements ComputationConfig.Fetcher {
  private static final Logger LOG =
      LoggerFactory.getLogger(StreamingEngineComputationConfigFetcher.class);
  private static final String CONFIG_REFRESHER_THREAD_NAME = "GlobalPipelineConfigRefresher";
  private static final int DEFAULT_WINDMILL_SERVICE_PORT = 443;
  private static final FluentBackoff BACKOFF_FACTORY =
      FluentBackoff.DEFAULT
          .withInitialBackoff(Duration.millis(100))
          .withMaxBackoff(Duration.standardMinutes(1))
          .withMaxCumulativeBackoff(Duration.standardMinutes(5));

  private final long globalConfigRefreshPeriodMillis;
  private final WorkUnitClient dataflowServiceClient;
  private final ScheduledExecutorService globalConfigRefresher;
  private final Consumer<StreamingEnginePipelineConfig> onStreamingConfig;
  private final AtomicBoolean hasReceivedGlobalConfig;

  private StreamingEngineComputationConfigFetcher(
      boolean hasReceivedGlobalConfig,
      long globalConfigRefreshPeriodMillis,
      WorkUnitClient dataflowServiceClient,
      ScheduledExecutorService globalConfigRefresher,
      Consumer<StreamingEnginePipelineConfig> onStreamingConfig) {
    this.globalConfigRefreshPeriodMillis = globalConfigRefreshPeriodMillis;
    this.dataflowServiceClient = dataflowServiceClient;
    this.globalConfigRefresher = globalConfigRefresher;
    this.onStreamingConfig = onStreamingConfig;
    this.hasReceivedGlobalConfig = new AtomicBoolean(hasReceivedGlobalConfig);
  }

  public static StreamingEngineComputationConfigFetcher create(
      long globalConfigRefreshPeriodMillis,
      WorkUnitClient dataflowServiceClient,
      Consumer<StreamingEnginePipelineConfig> onStreamingConfig) {
    return new StreamingEngineComputationConfigFetcher(
        /* hasReceivedGlobalConfig= */ false,
        globalConfigRefreshPeriodMillis,
        dataflowServiceClient,
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat(CONFIG_REFRESHER_THREAD_NAME).build()),
        onStreamingConfig);
  }

  @VisibleForTesting
  public static StreamingEngineComputationConfigFetcher forTesting(
      boolean hasReceivedGlobalConfig,
      long globalConfigRefreshPeriodMillis,
      WorkUnitClient dataflowServiceClient,
      Function<String, ScheduledExecutorService> executorSupplier,
      Consumer<StreamingEnginePipelineConfig> onStreamingConfig) {
    return new StreamingEngineComputationConfigFetcher(
        hasReceivedGlobalConfig,
        globalConfigRefreshPeriodMillis,
        dataflowServiceClient,
        executorSupplier.apply(CONFIG_REFRESHER_THREAD_NAME),
        onStreamingConfig);
  }

  @VisibleForTesting
  static MapTask createMapTask(StreamingComputationConfig computationConfig) {
    return new MapTask()
        .setSystemName(computationConfig.getSystemName())
        .setStageName(computationConfig.getStageName())
        .setInstructions(computationConfig.getInstructions());
  }

  private static Optional<StreamingConfigTask> fetchConfigWithRetry(
      ThrowingFetchWorkItemFn fetchWorkItemFn) {
    BackOff backoff = BACKOFF_FACTORY.backoff();
    while (true) {
      try {
        return fetchWorkItemFn
            .fetchWorkItem()
            .map(
                workItem -> {
                  StreamingConfigTask config = workItem.getStreamingConfigTask();
                  Preconditions.checkState(
                      config != null,
                      "Received invalid WorkItem without StreamingConfigTask. WorkItem={}",
                      workItem);
                  return config;
                });
      } catch (IllegalArgumentException | IOException e) {
        LOG.warn("Error fetching config: ", e);
        try {
          if (!BackOffUtils.next(Sleeper.DEFAULT, backoff)) {
            return Optional.empty();
          }
        } catch (IOException ioe) {
          LOG.warn("Error backing off, will not retry: ", ioe);
          return Optional.empty();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return Optional.empty();
        }
      }
    }
  }

  private static StreamingEnginePipelineConfig createPipelineConfig(StreamingConfigTask config) {
    StreamingEnginePipelineConfig.Builder pipelineConfig = StreamingEnginePipelineConfig.builder();
    if (config.getUserStepToStateFamilyNameMap() != null) {
      pipelineConfig.setUserStepToStateFamilyNameMap(config.getUserStepToStateFamilyNameMap());
    }

    if (config.getWindmillServiceEndpoint() != null
        && !config.getWindmillServiceEndpoint().isEmpty()) {
      int windmillPort =
          Optional.ofNullable(config.getWindmillServicePort())
              .filter(port -> port != 0)
              .map(Long::intValue)
              .orElse(DEFAULT_WINDMILL_SERVICE_PORT);

      ImmutableSet<HostAndPort> endpoints =
          StreamSupport.stream(
                  Splitter.on(',').split(config.getWindmillServiceEndpoint()).spliterator(),
                  /* isParallel= */ false)
              .map(endpoint -> HostAndPort.fromString(endpoint).withDefaultPort(windmillPort))
              .collect(toImmutableSet());

      pipelineConfig.setWindmillServiceEndpoints(endpoints);
    }

    if (config.getMaxWorkItemCommitBytes() != null
        && config.getMaxWorkItemCommitBytes() > 0
        && config.getMaxWorkItemCommitBytes() <= Integer.MAX_VALUE) {
      pipelineConfig.setMaxWorkItemCommitBytes(config.getMaxWorkItemCommitBytes().intValue());
    }

    return pipelineConfig.build();
  }

  private static Optional<ComputationConfig> createComputationConfig(StreamingConfigTask config) {
    return Optional.ofNullable(config.getStreamingComputationConfigs())
        .map(Iterables::getOnlyElement)
        .map(
            streamingComputationConfig ->
                ComputationConfig.create(
                    createMapTask(streamingComputationConfig),
                    streamingComputationConfig.getTransformUserNameToStateFamily(),
                    config.getUserStepToStateFamilyNameMap() != null
                        ? config.getUserStepToStateFamilyNameMap()
                        : ImmutableMap.of()));
  }

  @Override
  public void start() {
    fetchInitialPipelineGlobalConfig();
    schedulePeriodicGlobalConfigRequests();
  }

  @Override
  public Optional<ComputationConfig> fetchConfig(String computationId) {
    Preconditions.checkArgument(
        !computationId.isEmpty(),
        "computationId is empty. Cannot fetch computation config without a computationId.");
    return fetchConfigWithRetry(
            () -> dataflowServiceClient.getStreamingConfigWorkItem(computationId))
        .flatMap(StreamingEngineComputationConfigFetcher::createComputationConfig);
  }

  @Override
  public void stop() {
    // We have already shutdown or start has not been called.
    if (globalConfigRefresher.isShutdown() || !hasReceivedGlobalConfig.get()) {
      return;
    }

    globalConfigRefresher.shutdown();
    boolean isTerminated = false;
    try {
      isTerminated = globalConfigRefresher.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Error occurred shutting down: {}", globalConfigRefresher);
    }
    if (!isTerminated) {
      globalConfigRefresher.shutdownNow();
    }
  }

  /**
   * Initially b Schedules a background thread that periodically sends getConfig requests to
   * Dataflow Service to obtain the windmill service endpoint.
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  private void schedulePeriodicGlobalConfigRequests() {
    globalConfigRefresher.scheduleWithFixedDelay(
        () -> fetchGlobalConfig().ifPresent(onStreamingConfig),
        0,
        globalConfigRefreshPeriodMillis,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Blocks until we have received the initial global config from Dataflow or an exception is
   * thrown.
   */
  private synchronized void fetchInitialPipelineGlobalConfig() {
    while (!hasReceivedGlobalConfig.get()) {
      LOG.info("Sending request to get initial global configuration for this worker.");
      Optional<StreamingEnginePipelineConfig> globalConfig = fetchGlobalConfig();
      if (globalConfig.isPresent()) {
        onStreamingConfig.accept(globalConfig.get());
        hasReceivedGlobalConfig.set(true);
        break;
      }
      LOG.info("Haven't received initial global configuration, will retry in 5 seconds");
      sleepUninterruptibly(5, TimeUnit.SECONDS);
    }

    LOG.info("Initial global configuration received, harness is now ready");
  }

  private Optional<StreamingEnginePipelineConfig> fetchGlobalConfig() {
    return fetchConfigWithRetry(dataflowServiceClient::getGlobalStreamingConfigWorkItem)
        .map(StreamingEngineComputationConfigFetcher::createPipelineConfig);
  }

  @FunctionalInterface
  private interface ThrowingFetchWorkItemFn {
    Optional<WorkItem> fetchWorkItem() throws IOException;
  }
}
