package com.ias.aerospikescanner;

import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.ias.aerospikescanner.util.StatusUpdate;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Builder
@Slf4j
public class AerospikeClusterScanner {

    private final String workingDir;
    private final String host;
    private final String clusterName;
    private final String namespace;
    private final String set;
    private final String username;
    private final String password;

    private final List<Scanner> scanners = new ArrayList<>();

    private final AtomicInteger runningCount = new AtomicInteger(0);


    public void run() throws Exception {
        int port = 3000;
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.user = username;
        clientPolicy.password = password;
        AerospikeClient client = null;
        try {
            client = new AerospikeClient(clientPolicy, host, port);
            log.info("Starting to scan records from cluster " + clusterName);
            run(client, namespace, set);
            log.info("Finished Scanning records from cluster " + clusterName);
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
    public void run(AerospikeClient client, String namespace, String set) throws Exception {
        ScanPolicy policy = new ScanPolicy();
        policy.recordsPerSecond = 5000;

        List<String> nodes = client.getNodeNames();

        nodes.parallelStream().forEach(node -> {
            try {
                String outputFile = prepareDirectories() + "/keys." + clusterName + "." + node +".txt";
                FileOutputStream outputStream = new FileOutputStream(outputFile);
                log.info("\t\t Scanning and writing keys from node: " + node + " to: " + outputFile);
                Scanner s = new Scanner(outputStream, runningCount);
                scanners.add(s);
                client.scanNode(policy, node, namespace, set, s);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private String prepareDirectories() throws Exception {
        String keysDir = StatusUpdate.getClusterKeysPath(workingDir, namespace, set, clusterName);
        Files.createDirectories(Paths.get(keysDir));
        return keysDir;
    }

    @AllArgsConstructor
    private class Scanner implements ScanCallback {
        private final FileOutputStream outputStream;
        private final AtomicInteger runningCount;

        @Override
        public void scanCallback(Key key, Record record) throws AerospikeException {
            if(key.digest != null) {
                runningCount.incrementAndGet();
                String line = Buffer.bytesToHexString(key.digest) + "\n";
                try {
                    outputStream.write(line.getBytes());
                } catch (Exception e) {
                    throw new AerospikeException(e.getMessage());
                }
            }

            if(runningCount.get() % 1_000_000 == 0) {
                log.info(" Number of records fetched from cluster("+clusterName+"): " + runningCount.get());
            }
        }
    }
}
