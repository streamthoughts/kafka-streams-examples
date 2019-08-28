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

import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class CountProcessor<K, V> extends AbstractProcessor<K, V> {

    private final String storeName;

    private KeyValueStore<K, Long> store;

    /**
     * Creates a new {@link CountProcessor} instance.
     * @param storeName
     */
    public CountProcessor(final String storeName) {
        this.storeName = storeName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        super.init(context);
        store = (KeyValueStore<K, Long>)context.getStateStore(storeName);
    }

    @Override
    public void process(K key, V value) {
        // the keys should never be null
        if (key == null) {
            throw new StreamsException("Record key for count operator with state " + storeName + " should not be null.");
        }

        Long oldValue = store.get(key);
        Long newValue = null;

        if (value != null)  {
            newValue = (oldValue == null) ? 1L : oldValue + 1L;
        }

        if (newValue == null) {
            store.delete(key);
        } else {
            store.put(key, newValue);
        }

        context().forward(key, newValue);
    }

    @Override
    public void close() {

    }
}
