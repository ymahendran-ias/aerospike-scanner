package com.truecube.aerospikescanner;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.integralads.scores.ElasticacheFactory;
import com.integralads.scores.domain.Scores;
import com.integralads.scores.repository.elasticache.ElasticacheRedisWriteScoreRepository;
import com.integralads.scores.repository.elasticache.ElasticacheScoreSource;
import com.truecube.aerospikescanner.util.ElasticacheConfiguration;
import com.truecube.aerospikescanner.util.FSUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Builder
@Slf4j
public class AerospikeClusterScanner {

    private final String workingDir;
    private final String host;
    private final int port;
    private final String clusterName;
    private final String namespace = "scores";
    private final String set;
    private final String binName;
    private final String username = "<redacted>";
    private final String password = "<redacted>";

    private final ElasticacheRedisWriteScoreRepository writeScoreRepository;


    private final List<Scanner> scanners = new ArrayList<>();

    private final AtomicInteger runningCount = new AtomicInteger(0);

    private final ObjectMapper mapper = new ObjectMapper();

    private static final Map<String, ElasticacheScoreSource> sourceMap = Map.of(
            "file", new ElasticacheScoreSource("scores", "scores", false, "file"),
            "overrides", new ElasticacheScoreSource("scores", "score_overrides", false, "overrides"),
            "fast-track", new ElasticacheScoreSource("scores", "fast_track_scores", false, "fast-track"),
            "attention", new ElasticacheScoreSource("scores", "attNonMrc", false, "attention")
    );

    public static void main(String[] args) throws Exception {
        AerospikeClusterScanner fileScanner = new AerospikeClusterScanner("scores/file", "aero01.va.303net.net", 3000, "aero01",
                "cat_scores", "score", ElasticacheFactory.getWriteScoreRepository(
                new ElasticacheConfiguration(sourceMap.get("file")), sourceMap));
        AerospikeClusterScanner fastTrackScanner = new AerospikeClusterScanner("scores/fast-track", "aero01.va.303net.net", 3000, "aero01",
                "fast_track_scores", "scores", ElasticacheFactory.getWriteScoreRepository(
                new ElasticacheConfiguration(sourceMap.get("fast-track")), sourceMap));
        AerospikeClusterScanner overridesScanner = new AerospikeClusterScanner("scores/overrides", "aero01.va.303net.net", 3000, "aero01",
                "score_overrides", "scores", ElasticacheFactory.getWriteScoreRepository(
                new ElasticacheConfiguration(sourceMap.get("overrides")), sourceMap));
        AerospikeClusterScanner attentionScanner = new AerospikeClusterScanner("scores/attention", "aero01.va.303net.net", 3000, "aero01",
                "attNonMrc", "scores", ElasticacheFactory.getWriteScoreRepository(
                new ElasticacheConfiguration(sourceMap.get("attention")), sourceMap));

        List<AerospikeClusterScanner> scanners = List.of(
                fileScanner,
                fastTrackScanner,
                overridesScanner,
                attentionScanner
        );

        scanners.forEach(AerospikeClusterScanner::tryRun);
        scanners.forEach(AerospikeClusterScanner::close);
    }

    public void tryRun() {
        try {
            long startTime = System.nanoTime();
            run();
            long totalTime = System.nanoTime() - startTime;
            long timeSeconds = totalTime / 1000_000_000;
            long timeMinutes = timeSeconds / 60;
            long remainderSeconds = timeSeconds - (timeMinutes * 60);
            log.info("Transfer for set " + set + " took " + timeMinutes + ":" + timeSeconds + "." + remainderSeconds);
        } catch (Exception e) {
            log.error("Error occurred during run", e);
        }
    }

    public void run() throws Exception {
        ClientPolicy clientPolicy = new ClientPolicy();
        if(username != null && !"".equals(username)) {
            clientPolicy.user = username;
            clientPolicy.password = password;
        }
        AerospikeClient client = null;
        try {
            client = new AerospikeClient(clientPolicy, host, port);
            log.info("Starting to scan records from cluster " + clusterName);
            run(client, namespace, set, binName);
            log.info("Finished migrating records from cluster " + clusterName);
        } catch (AerospikeException e) {
            throw new Exception(String.format("Error while creating Aerospike " +
                    "client for %s:%d.", host, port), e);
        } finally {
            if (null != client) {
                client.close();
            }
        }
        scanners.forEach(s -> {
            try {
                s.outputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
    public void run(AerospikeClient client, String namespace, String set, String binName) throws Exception {
        ScanPolicy policy = new ScanPolicy();
        policy.recordsPerSecond = 5000;

        List<String> nodes = client.getNodeNames();

        nodes.parallelStream().forEach(node -> {
            try {
                String outputFile = prepareDirectories() + "/keys." + clusterName + "." + node +".txt";
                FileOutputStream outputStream = new FileOutputStream(outputFile);

                log.info("\t\t Scanning and writing keys from node: " + node + " to elasticache");
                Scanner s = new Scanner(outputStream, runningCount, binName);

                scanners.add(s);
                client.scanNode(policy, node, namespace, set, s);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private String prepareDirectories() throws Exception {
        String keysDir = FSUtil.getClusterKeysPath(workingDir, namespace, set, clusterName);
        Files.createDirectories(Paths.get(keysDir));
        return keysDir;
    }

    @AllArgsConstructor
    private class Scanner implements ScanCallback {
        private final FileOutputStream outputStream;
        private final AtomicInteger runningCount;
        private final String binName;

        @Override
        public void scanCallback(Key key, Record record) throws AerospikeException {
            if (key.digest != null) {
                runningCount.incrementAndGet();
                String keyDigest = Buffer.bytesToHexString(key.digest);
                try {
                    writeScoreRepository.addScores(keyDigest, Scores.builder()
                            .withScoresMap((Map<String, Integer>) record.getMap(binName)).build());
                } catch (Exception e) {
                    throw new AerospikeException(e.getMessage());
                }
            }

            if(runningCount.get() % 1_000_000 == 0) {
                log.info(" Number of records fetched from cluster("+clusterName+"): " + runningCount.get());
            }
        }
    }

    private void close() {
        writeScoreRepository.close();
    }
}
