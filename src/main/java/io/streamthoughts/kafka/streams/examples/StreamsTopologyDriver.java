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
package io.streamthoughts.kafka.streams.examples;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

/**
 * Default class to run a Kafka Streams application.
 */
public class StreamsTopologyDriver {

    private static final Logger LOG = LogManager.getLogger(StreamsTopologyDriver.class);

    public static void main(String[] args) {

        TopologyDriverConfig driverConfig = new TopologyDriverConfig();

        final Properties streamsConfig = driverConfig.streamsConfig();
        final Topology topology = driverConfig.topology();
        
        LOG.info("{}", topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, streamsConfig);
        // cleanup before starting
        if (driverConfig.cleanup()) {
            streams.cleanUp();
        }

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
           // do cleanup before stopping.
           if (driverConfig.cleanup()) {
               streams.cleanUp();
           }
           streams.close();
        }));
    }
}
