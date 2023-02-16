package com.truecube.aerospikescanner.util;

import org.apache.commons.cli.*;

public class CommandLineOptions {

    public static CommandLine loadArguments(String[] args) {
        Options options = loadArgumentOptions();
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null; //not good practice per se, but it serves its purpose
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            formatter.printHelp("utility-name", options);
            System.exit(1);
        }
        return cmd;
    }

    public static boolean isSKipDataFetch(CommandLine cmd) {
        return isSkip(cmd, SKIP_DATA_FETCH);
    }

    public static boolean isSKipDataCompare(CommandLine cmd) {
        return isSkip(cmd, SKIP_DATA_COMPARE);
    }

    private static boolean isSkip(CommandLine cmd, Option option) {
        String skip = cmd.getOptionValue(option);
        if(null != skip && skip.toLowerCase().equals("true")) {
            return true;
        }
        return false;
    }

    public static Options loadArgumentOptions() {
        Options options = new Options();

        options.addOption(WORKING_DIR);
        options.addOption(KOSHER_SERVER);
        options.addOption(KOSHER_PORT);
        options.addOption(KOSHER_USER);
        options.addOption(KOSHER_PASSWORD);
        options.addOption(KOSHER_CLUSTER_NAME);
        options.addOption(CULPRIT_SERVER);
        options.addOption(CULPRIT_PORT);
        options.addOption(CULPRIT_USER);
        options.addOption(CULPRIT_PASSWORD);
        options.addOption(CULPRIT_CULSTER_NAME);
        options.addOption(NAMESPACE);
        options.addOption(KOSHER_SET);
        options.addOption(CULPRIT_SET);

        options.getOptions().forEach(opt -> {
            opt.setRequired(true);
        });

        options.addOption(SKIP_DATA_COMPARE);
        options.addOption(SKIP_DATA_FETCH);

        return options;
    }


    public static final Option WORKING_DIR = new Option("wd", "workingDir", true, "Local Directory used to persist keys and outputs");
    public static final Option KOSHER_SERVER = new Option("kServer", "kosherServer", true, "hostname of the server that belongs to kosher cluster");
    public static final Option KOSHER_PORT = new Option("kPort", "kosherPort", true, "port of the server that belongs to kosher cluster");
    public static final Option KOSHER_USER = new Option("ksUser", "kosherServer.user", true, "username to read from kosher cluster");
    public static final Option KOSHER_PASSWORD = new Option("ksPwd", "kosherServer.password", true, "password to read from kosher cluster");
    public static final Option KOSHER_CLUSTER_NAME = new Option("kcn", "kosherCluster.name", true, "name for the kosher cluster - sub-directories will be created under this name");
    public static final Option CULPRIT_SERVER = new Option("cServer", "culpritServer", true, "hostname of the server that belongs to culprit cluster");
    public static final Option CULPRIT_PORT = new Option("cPort", "culpritPort", true, "port of the server that belongs to culprit cluster");
    public static final Option CULPRIT_USER = new Option("csUser", "culpritServer.user", true, "username to read from culprit cluster");
    public static final Option CULPRIT_PASSWORD = new Option("csPwd", "culpritServer.password", true, "password to read from culprit cluster");
    public static final Option CULPRIT_CULSTER_NAME = new Option("ccn", "culpritCluster.name", true, "name for the culprit cluster - sub-directories will be created under this name");
    public static final Option NAMESPACE = new Option("namespace", "namespace", true, "namespace to read from aerospike");
    public static final Option KOSHER_SET = new Option("kSet", "kosherSet", true, "set to read from kosher aerospike");

    public static final Option CULPRIT_SET = new Option("cSet", "culpritSet", true, "set to read from culprit aerospike");

    public static final Option SKIP_DATA_COMPARE = new Option("skipCompare", "skipDataCompare", true, "set this to true if you just want the keys from both clusters to be dumped into files and skip the keys comparison part");
    public static final Option SKIP_DATA_FETCH = new Option("skipFetch", "skipDataFetch", true, "set this to true if you already have the keys dumped to a file and just want to do the data comparison");

}
