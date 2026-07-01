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
package org.apache.beam.runners.dataflow.worker;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class WindmillSinkTest {
  @Mock StreamingModeExecutionContext mockContext;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testFlush() throws Exception {
    Windmill.WorkItemCommitRequest.Builder builder1 =
        Windmill.WorkItemCommitRequest.newBuilder()
            .setKey(ByteString.copyFromUtf8("key1"))
            .setWorkToken(1);
    Windmill.WorkItemCommitRequest.Builder builder2 =
        Windmill.WorkItemCommitRequest.newBuilder()
            .setKey(ByteString.copyFromUtf8("key2"))
            .setWorkToken(2);

    when(mockContext.getOutputBuilder()).thenReturn(builder1, builder2);
    when(mockContext.getSerializedKey())
        .thenReturn(ByteString.copyFromUtf8("key1"), ByteString.copyFromUtf8("key2"));
    when(mockContext.getMaxOutputKeyBytes()).thenReturn(1000L);
    when(mockContext.getMaxOutputValueBytes()).thenReturn(1000L);

    Map<String, Object> spec = new HashMap<>();
    spec.put(PropertyNames.OBJECT_TYPE_NAME, "WindmillSink");
    spec.put("stream_id", "stream1");
    CloudObject cloudSinkSpec = CloudObject.fromSpec(spec);
    WindmillSink.Factory factory = new WindmillSink.Factory();
    WindmillSink<String> sink =
        (WindmillSink<String>)
            factory.create(
                cloudSinkSpec,
                WindowedValues.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE),
                null,
                mockContext,
                null);

    Sink.SinkWriter<WindowedValue<String>> writer = sink.writer();

    // Write to builder1
    writer.add(WindowedValues.timestampedValueInGlobalWindow("e0", new Instant(0)));
    writer.flush();

    // Verify builder1 contains e0
    Windmill.WorkItemCommitRequest commit1 = builder1.build();
    assertEquals(1, commit1.getOutputMessagesCount());
    assertEquals("stream1", commit1.getOutputMessages(0).getDestinationStreamId());
    assertEquals(1, commit1.getOutputMessages(0).getBundlesCount());
    assertEquals(
        ByteString.copyFromUtf8("key1"), commit1.getOutputMessages(0).getBundles(0).getKey());
    assertEquals(1, commit1.getOutputMessages(0).getBundles(0).getMessagesCount());
    assertEquals(
        ByteString.copyFromUtf8("e0"),
        commit1.getOutputMessages(0).getBundles(0).getMessages(0).getData());

    // Write to builder2 (after flush, mockContext.getOutputBuilder() should return builder2)
    writer.add(WindowedValues.timestampedValueInGlobalWindow("e1", new Instant(1)));
    writer.flush();

    // Verify builder2 contains e1
    Windmill.WorkItemCommitRequest commit2 = builder2.build();
    assertEquals(1, commit2.getOutputMessagesCount());
    assertEquals("stream1", commit2.getOutputMessages(0).getDestinationStreamId());
    assertEquals(1, commit2.getOutputMessages(0).getBundlesCount());
    assertEquals(
        ByteString.copyFromUtf8("key2"), commit2.getOutputMessages(0).getBundles(0).getKey());
    assertEquals(1, commit2.getOutputMessages(0).getBundles(0).getMessagesCount());
    assertEquals(
        ByteString.copyFromUtf8("e1"),
        commit2.getOutputMessages(0).getBundles(0).getMessages(0).getData());

    // Verify builder1 was not modified during second write
    assertEquals(1, builder1.getOutputMessagesCount());
  }
}
