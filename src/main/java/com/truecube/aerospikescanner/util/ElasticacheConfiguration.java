package com.truecube.aerospikescanner.util;

import org.apache.commons.configuration.AbstractConfiguration;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.integralads.scores.repository.elasticache.ElasticacheScoreSource;

public class ElasticacheConfiguration extends AbstractConfiguration {

        private static Map<String, String> properties;

        public ElasticacheConfiguration(ElasticacheScoreSource elasticacheScoreSource) {
            properties = new HashMap<>();

            properties.put("score.repository.elasticache.writer", elasticacheScoreSource.getSourceName());
            properties.put("score.repository.elasticache.sources", elasticacheScoreSource.getSourceName());
            properties.put("score.repository.elasticache.sources.setName", elasticacheScoreSource.getSetName());
            properties.put("score.repository.elasticache.sources.rollupEnabled", String.valueOf(elasticacheScoreSource.isRollupScoresEnabled()));
            properties.put("score.repository.elasticache.sources.namespace", elasticacheScoreSource.getNamespace());
        }

        @Override
        protected void addPropertyDirect(String s, Object o) {
            if (o instanceof String) {
                properties.put(s, (String) o);
            }
        }

        @Override
        public boolean isEmpty() {
            return properties.isEmpty();
        }

        @Override
        public boolean containsKey(String s) {
            return properties.containsKey(s);
        }

        @Override
        public String getProperty(String s) {
            return properties.get(s);
        }

        @Override
        public Iterator<String> getKeys() {
            return properties.keySet().iterator();
        }
    }
