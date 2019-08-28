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
package io.streamthoughts.kafka.streams.examples.wordcount.topology;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;

public class WordCountStreamsTopologyTest {

    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";

    private TopologyTestDriver testDriver;

    @Before
    public void before() {
        System.out.println(Serdes.String().getClass().getName());
        WordCountStreamsTopology provider = new WordCountStreamsTopology();
        provider.configure(new HashMap<>() {{
            put(WordCountStreamsTopology.WordCountStreamsTopologyConfig.TOPIC_SOURCE_CONFIG, INPUT_TOPIC);
            put(WordCountStreamsTopology.WordCountStreamsTopologyConfig.TOPIC_SINK_CONFIG, OUTPUT_TOPIC);
        }});

        // Provide topology instance
        final Topology topology = provider.get();

        // Create a test configuration with mandatory props.
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-word-count");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        testDriver = new TopologyTestDriver(topology, props);
    }

    @After
    public void after() {

        testDriver.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test() {
        ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());

        testDriver.pipeInput(factory.create(INPUT_TOPIC, null,"I Heart Logs: Event Data, Stream Processing, and Data Integration"));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, null,"The Log: What every software engineer should know about real-time data's unifying abstraction"));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, null,"Making Sense of Stream Processing"));

        List<ProducerRecord<String, Long>> outputs = new LinkedList<>();
        ProducerRecord<String, Long> record;
        do {
            record = testDriver.readOutput(OUTPUT_TOPIC, new StringDeserializer(), new LongDeserializer());

            if (record != null) {
                outputs.add(record);
            }
        } while (record != null);

        Assert.assertFalse(outputs.isEmpty());

        Map<String, StateStore> allStateStores = testDriver.getAllStateStores();

        StateStore stateStore = testDriver.getStateStore("store-name");

        WindowStore<Object, Object> windowStore = testDriver.getWindowStore("window-store-name");

        KeyValueStore<String, ValueAndTimestamp<Long>> store = (KeyValueStore)testDriver.getAllStateStores().values().iterator().next();

        assertEquals(3L, store.get("data").value().longValue());
        assertEquals(2L, store.get("processing").value().longValue());
        assertEquals(1L, store.get("real-time").value().longValue());
    }
}