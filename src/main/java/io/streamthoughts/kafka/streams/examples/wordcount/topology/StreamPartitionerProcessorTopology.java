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

import io.streamthoughts.kafka.streams.examples.TopologyProvider;
import io.streamthoughts.kafka.streams.examples.wordcount.processor.FilterProcessor;
import io.streamthoughts.kafka.streams.examples.wordcount.processor.MapProcessor;
import io.streamthoughts.kafka.streams.examples.wordcount.processor.SplitProcessor;
import io.streamthoughts.kafka.streams.examples.wordcount.topology.internal.StopWords;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class StreamPartitionerProcessorTopology implements TopologyProvider {

    @Override
    public Topology get() {

        final Topology topology = new Topology();

        topology.addSource(Topology.AutoOffsetReset.EARLIEST, "processor-source", "topic-lines")

                .addProcessor("processor-split-into-words",
                        () -> new SplitProcessor<>("\\s"),
                        "processor-source")

                .addProcessor("processor-lowercase",
                        () -> new MapProcessor<Object, String>(kv -> KeyValue.pair(kv.key, kv.value.toLowerCase())),
                        "processor-split-into-words")

                .addProcessor("processor-filter-stop-words",
                        () -> new FilterProcessor<Object, String>((kv) -> StopWords.ENGLISH_STOP_WORDS.contains(kv.value), true),
                        "processor-lowercase");


        final StreamPartitioner<Object, String> partitioner = (topic, key, value, numPartitions) -> value.charAt(0) % numPartitions;

        topology.addSink("sink-processor", "topic-words", partitioner, "processor-filter-stop-words");

        return topology;
    }
}
