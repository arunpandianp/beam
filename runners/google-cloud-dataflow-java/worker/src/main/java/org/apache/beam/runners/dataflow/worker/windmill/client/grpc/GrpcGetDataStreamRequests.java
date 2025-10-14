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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.sdk.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility data classes for {@link GrpcGetDataStream}. */
final class GrpcGetDataStreamRequests {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcGetDataStreamRequests.class);
  private static final int STREAM_CANCELLED_ERROR_LOG_LIMIT = 3;

  private GrpcGetDataStreamRequests() {}

  private static String debugFormat(long value) {
    return String.format("%016x", value);
  }

  static class ComputationAndKeyRequest {
    private final String computation;
    private final KeyedGetDataRequest request;

    ComputationAndKeyRequest(String computation, KeyedGetDataRequest request) {
      this.computation = computation;
      this.request = request;
    }

    String getComputation() {
      return computation;
    }

    KeyedGetDataRequest getKeyedGetDataRequest() {
      return request;
    }
  }

  static class QueuedRequest {
    private final long id;
    private final @Nullable ComputationAndKeyRequest computationAndKeyRequest;
    private final @Nullable GlobalDataRequest globalDataRequest;
    private AppendableInputStream responseStream;

    private QueuedRequest(long id, GlobalDataRequest globalDataRequest, long deadlineSeconds) {
      this.id = id;
      this.computationAndKeyRequest = null;
      this.globalDataRequest = globalDataRequest;
      responseStream = new AppendableInputStream(deadlineSeconds);
    }

    private QueuedRequest(
        long id, ComputationAndKeyRequest computationAndKeyRequest, long deadlineSeconds) {
      this.id = id;
      this.computationAndKeyRequest = computationAndKeyRequest;
      this.globalDataRequest = null;
      responseStream = new AppendableInputStream(deadlineSeconds);
    }

    static QueuedRequest forComputation(
        long id,
        String computation,
        KeyedGetDataRequest keyedGetDataRequest,
        long deadlineSeconds) {
      return new QueuedRequest(
          id, new ComputationAndKeyRequest(computation, keyedGetDataRequest), deadlineSeconds);
    }

    static QueuedRequest global(
        long id, GlobalDataRequest globalDataRequest, long deadlineSeconds) {
      return new QueuedRequest(id, globalDataRequest, deadlineSeconds);
    }

    static Comparator<QueuedRequest> globalRequestsFirst() {
      return (QueuedRequest r1, QueuedRequest r2) -> {
        boolean r1gd = r1.getKind() == Kind.GLOBAL;
        boolean r2gd = r2.getKind() == Kind.GLOBAL;
        return r1gd == r2gd ? 0 : (r1gd ? -1 : 1);
      };
    }

    long id() {
      return id;
    }

    long byteSize() {
      if (globalDataRequest != null) {
        return globalDataRequest.getSerializedSize();
      }
      Preconditions.checkStateNotNull(computationAndKeyRequest);
      return 10L
          + computationAndKeyRequest.request.getSerializedSize()
          + computationAndKeyRequest.getComputation().length();
    }

    AppendableInputStream getResponseStream() {
      return responseStream;
    }

    void resetResponseStream() {
      this.responseStream = new AppendableInputStream(responseStream.getDeadlineSeconds());
    }

    enum Kind {
      COMPUTATION_AND_KEY_REQUEST,
      GLOBAL
    }

    Kind getKind() {
      return computationAndKeyRequest != null ? Kind.COMPUTATION_AND_KEY_REQUEST : Kind.GLOBAL;
    }

    ComputationAndKeyRequest getComputationAndKeyRequest() {
      return Preconditions.checkStateNotNull(computationAndKeyRequest);
    }

    GlobalDataRequest getGlobalDataRequest() {
      return Preconditions.checkStateNotNull(globalDataRequest);
    }

    void addToStreamingGetDataRequest(Windmill.StreamingGetDataRequest.Builder builder) {
      builder.addRequestId(id);
      switch (getKind()) {
        case COMPUTATION_AND_KEY_REQUEST:
          ComputationAndKeyRequest request = getComputationAndKeyRequest();
          builder
              .addStateRequestBuilder()
              .setComputationId(request.getComputation())
              .addRequests(request.request);
          break;
        case GLOBAL:
          builder.addGlobalDataRequest(getGlobalDataRequest());
          break;
      }
    }

    @Override
    public final String toString() {
      StringBuilder result = new StringBuilder("QueuedRequest{id=").append(id).append(", ");
      if (getKind() == Kind.GLOBAL) {
        result.append("GetSideInput=").append(getGlobalDataRequest());
      } else {
        KeyedGetDataRequest key = getComputationAndKeyRequest().request;
        result
            .append("KeyedGetState=[shardingKey=")
            .append(debugFormat(key.getShardingKey()))
            .append("cacheToken=")
            .append(debugFormat(key.getCacheToken()))
            .append("workToken")
            .append(debugFormat(key.getWorkToken()))
            .append("]");
      }
      return result.append('}').toString();
    }
  }

  /**
   * Represents a batch of queued requests. Methods are not thread-safe unless commented otherwise.
   */
  static class QueuedBatch {

    private final List<QueuedRequest> requests = new ArrayList<>();
    private final HashSet<Long> workTokens = new HashSet<>();
    private long byteSize = 0;
    private volatile boolean finalized = false;

    /** Returns a read-only view of requests. */
    List<QueuedRequest> requestsView() {
      return Collections.unmodifiableList(requests);
    }

    /**
     * Converts the batch to a {@link
     * org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetDataRequest}.
     */
    Windmill.StreamingGetDataRequest asGetDataRequest() {
      Windmill.StreamingGetDataRequest.Builder builder =
          Windmill.StreamingGetDataRequest.newBuilder();

      requests.stream()
          // Put all global data requests first because there is only a single repeated field for
          // request ids and the initial ids correspond to global data requests if they are present.
          .sorted(QueuedRequest.globalRequestsFirst())
          .forEach(request -> request.addToStreamingGetDataRequest(builder));

      return builder.build();
    }

    int requestsCount() {
      return requests.size();
    }

    boolean isFinalized() {
      return finalized;
    }

    void markFinalized() {
      finalized = true;
    }

    /** Adds a request to the batch. */
    boolean tryAddRequest(QueuedRequest request, int countLimit, long byteLimit) {
      if (finalized) {
        return false;
      }
      if (requests.size() >= countLimit) {
        return false;
      }
      long estimatedBytes = request.byteSize();
      if (byteSize + estimatedBytes >= byteLimit) {
        return false;
      }

      if (request.getKind() == QueuedRequest.Kind.COMPUTATION_AND_KEY_REQUEST
          && !workTokens.add(request.getComputationAndKeyRequest().request.getWorkToken())) {
        return false;
      }
      // At this point we have added to work items so we must accept the item.
      requests.add(request);
      byteSize += estimatedBytes;
      return true;
    }

    /** Let waiting for threads know that a failure occurred. */
    void notifyFailed() {
      for (QueuedRequest request : requests) {
        request.getResponseStream().cancel();
      }
    }
  }
}
