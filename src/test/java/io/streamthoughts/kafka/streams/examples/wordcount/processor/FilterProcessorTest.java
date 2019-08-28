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
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.function.Predicate;

import static org.junit.Assert.*;

public class FilterProcessorTest {


    private MockProcessorContext context = new MockProcessorContext();
    private FilterProcessor<Object, String> processor;

    @Before
    public void before() {
        Predicate<KeyValue<Object, String>> predicate = kv -> kv.value != null && !kv.value.isEmpty();
        processor = new FilterProcessor<>(predicate, false);
    }

    @After
    public void after() {
        context.resetForwards();
    }

    @Test
    public void testTrue() {
        processor.init(context);
        processor.process(null, "value");

        final Iterator<MockProcessorContext.CapturedForward> forwarded = context.forwarded().iterator();

        assertTrue(forwarded.hasNext());
        MockProcessorContext.CapturedForward captured = forwarded.next();
        assertEquals("value", captured.keyValue().value);
    }

    @Test
    public void testNullValue() {
        processor.init(context);
        processor.process(null, null);

        final Iterator<MockProcessorContext.CapturedForward> forwarded = context.forwarded().iterator();
        assertFalse(forwarded.hasNext());
    }

}