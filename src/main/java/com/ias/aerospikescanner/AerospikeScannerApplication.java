package com.ias.aerospikescanner;

import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
public class AerospikeScannerApplication {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(AerospikeScannerApplication.class, args);
//        init();
        new ScanSeries().run();
    }

    private static void init() throws Exception{
        String namespace = "scores";
        String host = "aero01.jp.303net.net";
        String user = "scores-ro";
        String password = "kEPO38HRMqRxlGddlDSS6DU";
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
            Statement stmnt = new Statement();
            stmnt.setNamespace(namespace);
            stmnt.setSetName("cat_scores");
            RecordSet rs = client.query(null, stmnt);
            while (rs.next()) {
                Key key = rs.getKey();
                Record record = rs.getRecord();
                Value v = key.userKey;
                System.out.println(v.toString());
                System.out.println(record.toString());
                count++;
                if(count % 100 == 0) {
                    System.out.println("total count now: " + count);
                    return;
                }
            }
            System.out.println("total count " + count);
        } catch (AerospikeException e) {
            throw new Exception(String.format("Error while creating Aerospike " +
                    "client for %s:%d.", host, port), e);
        } finally {
            if (null != client) {
                client.close();
            }
        }
    }


}
