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
import io.streamthoughts.kafka.streams.examples.wordcount.processor.CountProcessor;
import io.streamthoughts.kafka.streams.examples.wordcount.processor.FilterProcessor;
import io.streamthoughts.kafka.streams.examples.wordcount.processor.MapProcessor;
import io.streamthoughts.kafka.streams.examples.wordcount.processor.SplitProcessor;
import io.streamthoughts.kafka.streams.examples.wordcount.topology.internal.StopWords;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.HashMap;

public class WordCountProcessorTopology implements TopologyProvider {

    private static final String WORD_COUNT_STORE_NAME = "WordCount";
    private static final String PROCESSOR_SPLIT_INTO_WORDS = "processor-split-into-words";
    private static final String PROCESSOR_FILTER_STOP_WORDS = "processor-filter-stop-words";
    private static final String PROCESSOR_WORD_SOURCE = "processor-word-source";
    private static final String WORD_COUNT_PROCESSOR = "word-count-processor";
    private static final String PROCESSOR_SOURCE = "processor-source";
    private static final String PROCESSOR_LOWERCASE = "processor-lowercase";
    private static final String PROCESSOR_REKEYED = "processor-rekeyed";

    @Override
    public Topology get() {
        final Topology topology = new Topology();

        topology
            .addSource(Topology.AutoOffsetReset.EARLIEST, PROCESSOR_SOURCE, "topic-lines")

            .addProcessor(PROCESSOR_SPLIT_INTO_WORDS,
                    () -> new SplitProcessor<>("\\s"),
                    PROCESSOR_SOURCE)

            .addProcessor(PROCESSOR_LOWERCASE,
                    () -> new MapProcessor<Object, String>(kv -> KeyValue.pair(kv.key, kv.value.toLowerCase())),
                    PROCESSOR_SPLIT_INTO_WORDS)

            .addProcessor(PROCESSOR_FILTER_STOP_WORDS,
                    () -> new FilterProcessor<Object, String>((kv) -> StopWords.ENGLISH_STOP_WORDS.contains(kv.value), true),
                    PROCESSOR_LOWERCASE)

            .addProcessor(PROCESSOR_REKEYED,
                    () -> new MapProcessor<Object, String>(kv -> KeyValue.pair(kv.value, kv.value)),
                    PROCESSOR_FILTER_STOP_WORDS)

            .addSink("processor-word-sink", "topic-words", PROCESSOR_REKEYED)

            .addSource(Topology.AutoOffsetReset.LATEST, PROCESSOR_WORD_SOURCE, "topic-words")

            .addProcessor(WORD_COUNT_PROCESSOR,
                    () -> new CountProcessor<Object, String>(WORD_COUNT_STORE_NAME) , PROCESSOR_WORD_SOURCE)

            .addSink("processor-count-sink-", "topic-word-count", WORD_COUNT_PROCESSOR);

        StoreBuilder<KeyValueStore<String, Long>> counts = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(WORD_COUNT_STORE_NAME),
            Serdes.String(),
            Serdes.Long())
            .withLoggingEnabled(new HashMap<>());

        topology.addStateStore(counts, WORD_COUNT_PROCESSOR);

        return topology;
    }
}
