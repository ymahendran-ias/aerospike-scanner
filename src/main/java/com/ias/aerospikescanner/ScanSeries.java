package com.ias.aerospikescanner;

import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;

import java.io.FileOutputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ScanSeries implements ScanCallback {

    private AtomicInteger count = new AtomicInteger(0);

    FileOutputStream outputStream = null;

    public void run() throws Exception {
        outputStream = new FileOutputStream("/Users/ymahendran/Desktop/scores.au.txt");
        String namespace = "scores";
//        String set = "adenvironment";
        String set = "cat_scores";
        String host = "aero01.au.303net.net";
        String user = "scores-ro";
        String password = "";
        int port = 3000;
        ClientPolicy clientPolicy = new ClientPolicy();
        if (user != null && password != null) {
            clientPolicy.user = user;
            clientPolicy.password = password;
        }
        AerospikeClient client = null;
        try {
            long count = 0;
            client = new AerospikeClient(clientPolicy, host, port);
            run(client, namespace, set);
            System.out.println("<<<<<<<<<<<<<<<< done running >>>>>>>>>>>");
        } catch (AerospikeException e) {
            throw new Exception(String.format("Error while creating Aerospike " +
                    "client for %s:%d.", host, port), e);
        } finally {
            if (null != client) {
                client.close();
            }
        }
    }
    public void run(AerospikeClient client, String namespace, String set) {
        ScanPolicy policy = new ScanPolicy();
        policy.recordsPerSecond = 5000;

        List<String> nodes = client.getNodeNames();
        long begin = System.currentTimeMillis();

        for (String nodeName : nodes) {
            client.scanNode(policy, nodeName, namespace, set, this);
            System.out.println("scanning node: " + nodeName);
            break;
        }
    }

    @Override
    public void scanCallback(Key key, Record record) throws AerospikeException {
        if(key.digest != null) {
            count.incrementAndGet();

//            System.out.println("record: " + record.toString());
//            System.out.println(record.bins.get("envScores"));

            String line = key.digest + "\t" + record.bins.get("score") + "\n";
//            line = record.expiration + " :: " + record.getTimeToLive();
//            System.out.println(line);
            try {
                writeToFile(line);
            } catch (Exception e) {
                throw new AerospikeException(e.getMessage());
            }
        }

        if(count.get() > 10_000_000) {
            System.exit(0);
            try {
                close();
            } catch (Exception e) {
                throw new AerospikeException(e.getMessage());
            }
        }
    }

    private void close() throws Exception {
        outputStream.close();
    }

    private void writeToFile(String record) throws Exception {
        outputStream.write(record.getBytes());
    }
}
