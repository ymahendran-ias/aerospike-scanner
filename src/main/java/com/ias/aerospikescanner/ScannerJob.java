package com.ias.aerospikescanner;

import com.ias.aerospikescanner.util.CommandLineOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;

import java.util.ArrayList;
import java.util.List;


@Slf4j
public class ScannerJob {

    public static void main(String[] args) throws Exception {

        CommandLine cmd = CommandLineOptions.loadArguments(args);
        boolean skipDataFetch = CommandLineOptions.isSKipDataFetch(cmd);
        boolean skipDataCompare = CommandLineOptions.isSKipDataCompare(cmd);
        if(skipDataFetch && skipDataCompare) {
            log.info("You have asked to skip both dataFetch and dataCompare.  So I am going to sleep! Bye!");
            System.exit(0);
        }

        if(!skipDataFetch) {
            List<AerospikeClusterScanner> scanners = new ArrayList<>();
            scanners.add(getClusterScanner(true, cmd));
            scanners.add(getClusterScanner(false, cmd));
            scanners.parallelStream().forEach(scanner -> {
                try {
                    scanner.run();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } else {
            log.info("Skipping data fetch!");
        }
        if(!skipDataCompare) {
            KeysComparor kc = new KeysComparor(
                    cmd.getOptionValue("workingDir"), cmd.getOptionValue("namespace"), cmd.getOptionValue("set"),
                    cmd.getOptionValue(CommandLineOptions.KOSHER_CLUSTER_NAME), cmd.getOptionValue(CommandLineOptions.CULPRIT_CULSTER_NAME));
            kc.process();
        }

    }

    private static AerospikeClusterScanner getClusterScanner(boolean isKosher, CommandLine cmd) {
        Option host = isKosher ? CommandLineOptions.KOSHER_SERVER : CommandLineOptions.CULPRIT_SERVER;
        Option cn = isKosher ? CommandLineOptions.KOSHER_CLUSTER_NAME : CommandLineOptions.CULPRIT_CULSTER_NAME;
        Option user = isKosher ? CommandLineOptions.KOSHER_USER : CommandLineOptions.CULPRIT_USER;
        Option pwd = isKosher ? CommandLineOptions.KOSHER_PASSWORD : CommandLineOptions.CULPRIT_PASSWORD;
        return AerospikeClusterScanner.builder()
                .workingDir(cmd.getOptionValue(CommandLineOptions.WORKING_DIR))
                .host(cmd.getOptionValue(host))
                .clusterName(cmd.getOptionValue(cn))
                .namespace(cmd.getOptionValue("namespace"))
                .set(cmd.getOptionValue("set"))
                .username(cmd.getOptionValue(user))
                .password(cmd.getOptionValue(pwd))
                .build();
    }


}
