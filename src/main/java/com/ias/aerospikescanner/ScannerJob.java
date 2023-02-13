package com.ias.aerospikescanner;

import com.ias.aerospikescanner.util.CommandLineOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;


@Slf4j
public class ScannerJob {

    public static void main(String[] args) throws Exception {

        CommandLine cmd = CommandLineOptions.loadArguments(args);
        AerospikeClusterScanner kScanner = getClusterScanner(true, cmd);
        AerospikeClusterScanner cScanner = getClusterScanner(false, cmd);

        kScanner.run();
        cScanner.run();

        KeysComparor kc = new KeysComparor(cmd.getOptionValue("workingDir"), cmd.getOptionValue("namespace"), cmd.getOptionValue("set"), cmd.getOptionValue("kosherClusterName"), cmd.getOptionValue("culpritClusterName"));
        kc.process();

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
