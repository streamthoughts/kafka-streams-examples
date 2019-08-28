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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.streamthoughts.kafka.streams.examples.rocksdb.EnableStatisticRocksDBConfigSetter;
import io.streamthoughts.kafka.streams.examples.utils.Utils;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Map;
import java.util.Properties;

import static io.streamthoughts.kafka.streams.examples.utils.ConfigUtils.toMap;
import static io.streamthoughts.kafka.streams.examples.utils.ConfigUtils.toProperties;
import static io.streamthoughts.kafka.streams.examples.utils.Utils.getClassLoader;

public class TopologyDriverConfig {

    private static final String STREAMS_CONFIG = "streams";
    private static final String ROCKSDB_CONFIG = "rocksdb";
    private static final String CLEANUP_CONFIG = "cleanup";
    private static final String TOPOLOGY_CONFIG = "topology";
    private static final String TOPOLOGY_PROVIDER_CONFIG = "config";
    private static final String TOPOLOGY_PROVIDER_CLASS_CONFIG = "class";

    private final Config conf;

    /**
     * Creates a new {@link TopologyDriverConfig} instance.
     */
    public TopologyDriverConfig() {
        conf = ConfigFactory.load();
    }

    public boolean cleanup() {
        return conf.hasPath(CLEANUP_CONFIG) && conf.getBoolean(CLEANUP_CONFIG);
    }

    public Properties streamsConfig() {
        Properties streamsConfig = toProperties(conf.getConfig(STREAMS_CONFIG));

        if (conf.hasPath(ROCKSDB_CONFIG)) {
            Config rocksdbConfig = conf.getConfig(ROCKSDB_CONFIG);
            if (rocksdbConfig.hasPath("stats.enable") && rocksdbConfig.getBoolean("stats.enable")) {
                streamsConfig.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, EnableStatisticRocksDBConfigSetter.class);
            }
        }
        return streamsConfig;
    }

    public Topology topology() {
        Config topologyConfig = conf.getConfig(TOPOLOGY_CONFIG);

        Map<String, Object> topologyProviderConfig = null;
        if (topologyConfig.hasPath(TOPOLOGY_PROVIDER_CONFIG)) {
            Config config = topologyConfig.getConfig(TOPOLOGY_PROVIDER_CONFIG);
            topologyProviderConfig = toMap(config);
        }

        final String trimmed = topologyConfig.getString(TOPOLOGY_PROVIDER_CLASS_CONFIG).trim();

        try {
            final Class<?> c = Class.forName(trimmed, true, getClassLoader());
            TopologyProvider provider = getConfiguredInstance(topologyProviderConfig, c, TopologyProvider.class);
            if (provider == null) return null;
            return provider.get();
        } catch (ClassNotFoundException e) {
            throw new TopologyDriverException("Error while creating new instance of " + trimmed, e);
        }

    }

    private TopologyProvider getConfiguredInstance(final Map<String, Object> configs,
                                                   final Class<?> c,
                                                   final Class<TopologyProvider> expected) {
        if (c == null)
            return null;
        Object o = Utils.newInstance(c);
        if (!expected.isInstance(o))
            throw new TopologyDriverException(c.getName() + " is not an instance of " + expected.getName());
        if (o instanceof Configurable)
            ((Configurable) o).configure(configs);
        return expected.cast(o);
    }

}
