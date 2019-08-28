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
import io.streamthoughts.kafka.streams.examples.wordcount.topology.internal.StopWords;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class WordCountStreamsTopology implements TopologyProvider, Configurable {

    private WordCountStreamsTopologyConfig config;

    @Override
    public Topology get() {
        if (config == null) {
            throw new IllegalArgumentException("Topology provider 'WordCountStreamsTopology' must be configured!");
        }

        final StreamsBuilder builder = new StreamsBuilder();
        final TimestampExtractor extractor = (record, previousTimestamp) -> record.timestamp();

        Consumed<String, String> consumed = Consumed
            .with(Serdes.String(), Serdes.String())
            .withTimestampExtractor(extractor);

        KStream<String, String> words = builder.stream(config.source(), consumed)
                .flatMapValues(new SplitLineAndLowercase())
                .filterNot((k, v) -> StopWords.ENGLISH_STOP_WORDS.contains(v));

        words.groupBy((k, v) -> v)
                .count()
                .toStream()
                .to(config.sink(), Produced.with(Serdes.String(), Serdes.Long()));

        final Topology topology = builder.build();
        return topology;
    }

    static class SplitLineAndLowercase implements ValueMapper<String, Iterable<String>> {

        private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s");

        @Override
        public Iterable<String> apply(final String value) {
            String cleaned = value.replaceAll("[,':;]", " ");
            return Arrays.stream(WHITESPACE_PATTERN.split(cleaned))
                    .map(String::toLowerCase)
                    .collect(Collectors.toList());
        }
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        config = new WordCountStreamsTopologyConfig(configs);
    }

    public static class WordCountStreamsTopologyConfig extends AbstractConfig {

        public static final String TOPIC_SOURCE_CONFIG = "topic.source";
        public static final String TOPIC_SINK_CONFIG = "topic.sink";
        public static final String COUNT_STORE_NAME = "store.count.name";

        WordCountStreamsTopologyConfig(final Map<?, ?> originals) {
            super(configDef(), originals, Collections.emptyMap(), false);
        }

        static ConfigDef configDef() {
            return new ConfigDef()
                    .define(TOPIC_SOURCE_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                            "The source topic")
                    .define(TOPIC_SINK_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                            "The sink topic")
                    .define(COUNT_STORE_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                            "The store name used for counts");
        }

        String source() {
            return getString(TOPIC_SOURCE_CONFIG);
        }

        String sink() {
            return getString(TOPIC_SINK_CONFIG);
        }

        String store() {
            return getString(COUNT_STORE_NAME);
        }
    }
}
