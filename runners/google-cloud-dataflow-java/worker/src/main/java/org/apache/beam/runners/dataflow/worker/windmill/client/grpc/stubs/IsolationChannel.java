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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import net.jcip.annotations.GuardedBy;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.CallOptions;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ClientCall;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Metadata;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.MethodDescriptor;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Status;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;

/**
 * A {@link ManagedChannel} that creates a dynamic # of cached channels to the same endpoint such
 * that each active rpc has its own channel.
 */
@Internal
class IsolationChannel extends ManagedChannel {
  static final Logger LOG = Logger.getLogger(IsolationChannel.class.getName());

  private final Supplier<ManagedChannel> channelFactory;

  @GuardedBy("channelCache")
  private final List<ManagedChannel> channelCache;

  @GuardedBy("channelCache")
  private final Set<ManagedChannel> usedChannels;

  @GuardedBy("channelCache")
  private boolean shutdownStarted = false;

  private final String authority;

  // Expected that supplier returns channels to the same endpoint with the same authority.
  public static IsolationChannel create(Supplier<ManagedChannel> channelFactory) {
    return new IsolationChannel(channelFactory);
  }

  @VisibleForTesting
  IsolationChannel(Supplier<ManagedChannel> channelFactory) {
    this.channelFactory = channelFactory;
    this.channelCache = new ArrayList<>();
    ManagedChannel channel = channelFactory.get();
    this.authority = channel.authority();
    this.channelCache.add(channel);
    this.usedChannels = new HashSet<>();
  }

  @Override
  public String authority() {
    return authority;
  }

  /** Pick an unused channel from the set or create one and then create a {@link ClientCall}. */
  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions) {
    ManagedChannel channel = getChannel();
    return new ReleasingClientCall<>(this, channel, channel.newCall(methodDescriptor, callOptions));
  }

  private ManagedChannel getChannel() {
    synchronized (channelCache) {
      if (!channelCache.isEmpty()) {
        ManagedChannel result = channelCache.remove(channelCache.size() - 1);
        usedChannels.add(result);
        return result;
      }
    }
    ManagedChannel result = channelFactory.get();
    synchronized (channelCache) {
      usedChannels.add(result);
      if (shutdownStarted) result.shutdown();
    }
    return result;
  }

  @Override
  public ManagedChannel shutdown() {
    synchronized (channelCache) {
      shutdownStarted = true;
      for (ManagedChannel channel : usedChannels) {
        channel.shutdown();
      }
      for (ManagedChannel channel : channelCache) {
        channel.shutdown();
      }
    }
    return this;
  }

  @Override
  public boolean isShutdown() {
    synchronized (channelCache) {
      if (!shutdownStarted) return false;
      for (ManagedChannel channel : usedChannels) {
        if (!channel.isShutdown()) return false;
      }
      for (ManagedChannel channel : channelCache) {
        if (!channel.isShutdown()) return false;
      }
    }
    return true;
  }

  @Override
  public boolean isTerminated() {
    synchronized (channelCache) {
      if (!shutdownStarted) return false;
      for (ManagedChannel channel : usedChannels) {
        if (!channel.isTerminated()) return false;
      }
      for (ManagedChannel channel : channelCache) {
        if (!channel.isTerminated()) return false;
      }
    }
    return true;
  }

  @Override
  public ManagedChannel shutdownNow() {
    synchronized (channelCache) {
      shutdownStarted = true;
      for (ManagedChannel channel : usedChannels) {
        channel.shutdownNow();
      }
      for (ManagedChannel channel : channelCache) {
        channel.shutdownNow();
      }
    }
    return this;
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long endTimeNanos = System.nanoTime() + unit.toNanos(timeout);
    if (isTerminated()) return true;
    synchronized (channelCache) {
      if (!shutdownStarted) return false;
      for (ManagedChannel channel : usedChannels) {
        long awaitTimeNanos = endTimeNanos - System.nanoTime();
        if (awaitTimeNanos <= 0) {
          break;
        }
        channel.awaitTermination(awaitTimeNanos, TimeUnit.NANOSECONDS);
      }
      for (ManagedChannel channel : channelCache) {
        long awaitTimeNanos = endTimeNanos - System.nanoTime();
        if (awaitTimeNanos <= 0) {
          break;
        }
        channel.awaitTermination(awaitTimeNanos, TimeUnit.NANOSECONDS);
      }
    }
    return isTerminated();
  }

  void releaseChannelAfterCall(ManagedChannel managedChannel) {
    synchronized (channelCache) {
      Preconditions.checkState(
          usedChannels.remove(managedChannel), "Channel released that was not used");
      channelCache.add(managedChannel);
    }
  }

  /** ClientCall wrapper that adds channel back to the cache on completion. */
  private static class ReleasingClientCall<ReqT, RespT>
      extends SimpleForwardingClientCall<ReqT, RespT> {
    private final IsolationChannel isolationChannel;

    private final ManagedChannel channel;

    @Nullable private CancellationException cancellationException;
    private final AtomicBoolean wasClosed = new AtomicBoolean();
    private final AtomicBoolean wasReleased = new AtomicBoolean();

    public ReleasingClientCall(
        IsolationChannel isolationChannel,
        ManagedChannel channel,
        ClientCall<ReqT, RespT> delegate) {
      super(delegate);
      this.isolationChannel = isolationChannel;
      this.channel = channel;
    }

    @Override
    public void start(Listener<RespT> responseListener, Metadata headers) {
      if (cancellationException != null) {
        throw new IllegalStateException("Call is already cancelled", cancellationException);
      }
      try {
        super.start(
            new SimpleForwardingClientCallListener<RespT>(responseListener) {
              @Override
              public void onClose(Status status, Metadata trailers) {
                if (!wasClosed.compareAndSet(false, true)) {
                  LOG.log(
                      Level.WARNING,
                      "Call is being closed more than once. Please make sure that onClose() is not being manually called.");
                  return;
                }
                try {
                  super.onClose(status, trailers);
                } finally {
                  if (wasReleased.compareAndSet(false, true)) {
                    isolationChannel.releaseChannelAfterCall(channel);
                  } else {
                    LOG.log(
                        Level.WARNING,
                        "Entry was released before the call is closed. This may be due to an exception on start of the call.");
                  }
                }
              }
            },
            headers);
      } catch (Exception e) {
        // In case start failed, make sure to release
        if (wasReleased.compareAndSet(false, true)) {
          isolationChannel.releaseChannelAfterCall(channel);
        } else {
          LOG.log(
              Level.WARNING,
              "The entry is already released. This indicates that onClose() has already been called previously");
        }
        throw e;
      }
    }

    @Override
    public void cancel(@Nullable String message, @Nullable Throwable cause) {
      this.cancellationException = new CancellationException(message);
      super.cancel(message, cause);
    }
  }
}
