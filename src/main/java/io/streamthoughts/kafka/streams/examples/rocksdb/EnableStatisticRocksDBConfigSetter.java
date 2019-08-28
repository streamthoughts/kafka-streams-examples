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
package io.streamthoughts.kafka.streams.examples.rocksdb;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;

import java.util.Collections;
import java.util.Map;

/**
 * Simple {@link RocksDBConfigSetter} to enable RocksDB logs and statistics.
 */
public class EnableStatisticRocksDBConfigSetter implements RocksDBConfigSetter {

    private Statistics statistics;

    @Override
    public void setConfig(final String storeName,
                          final Options options,
                          final Map<String, Object> configs) {

        final EnableStatisticRocksDBConfig config = new EnableStatisticRocksDBConfig(configs);

        statistics = new Statistics();
        statistics.setStatsLevel(StatsLevel.ALL);
        options.setStatistics(statistics);
        options.setStatsDumpPeriodSec(config.dumpPeriodSec());
        options.setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
        options.setDbLogDir(config.logDir());
        options.setMaxLogFileSize(config.maxLogFileSize());
    }

    @Override
    public void close(final String storeName, final Options options) {
        if (statistics != null) {
            statistics.close(); /* extends RocksObject */
        }
    }

    public static class EnableStatisticRocksDBConfig extends AbstractConfig {

        public static final String ROCKSDB_STATS_DUMP_PERIOD_SEC_CONFIG = "rocksdb.stats.dumpPeriodSec";
        private static final int DEFAULT_STATS_DUMP_PERIOD = 30 ;

        public static final String ROCKSDB_MAX_LOG_FILE_SIZE_CONFIG = "rocksdb.log.max.file.size";
        private static final int MAX_LOG_FILE_SIZE  = 100 * 1024 * 1024; // 100MB

        public static final String ROCKSDB_LOG_DIR_CONFIG = "rocksdb.log.dir";
        private static final String DEFAULT_LOG_DIR = "/tmp/rocksdb-statistics-logs";

        EnableStatisticRocksDBConfig(final Map<String, ?> originals) {
            super(configDef(), originals, Collections.emptyMap(), false);
        }

        int dumpPeriodSec() {
            return getInt(ROCKSDB_STATS_DUMP_PERIOD_SEC_CONFIG);
        }

        String logDir() {
            return getString(ROCKSDB_LOG_DIR_CONFIG);
        }

        int maxLogFileSize() {
            return getInt(ROCKSDB_MAX_LOG_FILE_SIZE_CONFIG);
        }

        static ConfigDef configDef() {
            return new ConfigDef()
                    .define(ROCKSDB_LOG_DIR_CONFIG, ConfigDef.Type.STRING, DEFAULT_LOG_DIR,
                            ConfigDef.Importance.HIGH, "The RocksDB log directory.")

                    .define(ROCKSDB_STATS_DUMP_PERIOD_SEC_CONFIG, ConfigDef.Type.INT, DEFAULT_STATS_DUMP_PERIOD,
                            ConfigDef.Importance.HIGH, "The RocksDB statistics dump period in seconds.")

                    .define(ROCKSDB_MAX_LOG_FILE_SIZE_CONFIG, ConfigDef.Type.INT, MAX_LOG_FILE_SIZE,
                            ConfigDef.Importance.HIGH, "The RocksDB maximum log file size.");
        }
    }
}