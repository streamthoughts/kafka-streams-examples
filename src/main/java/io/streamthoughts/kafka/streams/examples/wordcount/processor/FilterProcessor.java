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
package io.streamthoughts.kafka.streams.examples.wordcount.processor;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;

import java.util.function.Predicate;

/**
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class FilterProcessor<K, V> extends AbstractProcessor<K, V> {

    private final Predicate<KeyValue<K, V>> predicate;
    private final boolean filterNot;

    /**
     * Creates a new {@link FilterProcessor} instance.
     * @param predicate    the predicate to be used.
     */
    public FilterProcessor(final Predicate<KeyValue<K, V>> predicate,
                           final boolean filterNot) {
        this.predicate = predicate;
        this.filterNot = filterNot;
    }

    @Override
    public void process(K k, V v) {
        if (filterNot ^ predicate.test(KeyValue.pair(k, v))) {
            context().forward(k, v);
        }
    }
}
