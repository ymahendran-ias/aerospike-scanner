package com.ias.aerospikescanner.util;

import org.apache.commons.cli.*;

public class CommandLineOptions {

    public static CommandLine loadArguments(String[] args) {
        Options options = loadArgumentOptions();
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null; //not a good practice, it serves its purpose
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
        }
        return cmd;
    }

    public static final Option WORKING_DIR = new Option("wd", "workingDir", true, "Local Directory used to persist keys and outputs");
    public static final Option KOSHER_SERVER = new Option("kServer", "kosherServer", true, "hostname of the server that belongs to kosher cluster");
    public static final Option KOSHER_USER = new Option("ksUser", "kosherServer.user", true, "username to read from kosher cluster");
    public static final Option KOSHER_PASSWORD = new Option("ksPwd", "kosherServer.password", true, "password to read from kosher cluster");
    public static final Option KOSHER_CLUSTER_NAME = new Option("kcn", "kosherCluster.name", true, "name for the kosher cluster - sub-directories will be created under this name");
    public static final Option CULPRIT_SERVER = new Option("cServer", "culpritServer", true, "hostname of the server that belongs to culprit cluster");
    public static final Option CULPRIT_USER = new Option("csUser", "culpritServer.user", true, "username to read from culprit cluster");
    public static final Option CULPRIT_PASSWORD = new Option("csPwd", "culpritServer.password", true, "password to read from culprit cluster");
    public static final Option CULPRIT_CULSTER_NAME = new Option("ccn", "culpritCluster.name", true, "name for the culprit cluster - sub-directories will be created under this name");
    public static final Option NAMESPACE = new Option("namespace", "Namespace", true, "namespace to read from aerospike");
    public static final Option SET = new Option("set", "set", true, "set to read from aerospike");

    public static Options loadArgumentOptions() {
        Options options = new Options();

        options.addOption(WORKING_DIR);
        options.addOption(KOSHER_SERVER);
        options.addOption(KOSHER_USER);
        options.addOption(KOSHER_PASSWORD);
        options.addOption(KOSHER_CLUSTER_NAME);
        options.addOption(CULPRIT_SERVER);
        options.addOption(CULPRIT_USER);
        options.addOption(CULPRIT_PASSWORD);
        options.addOption(CULPRIT_CULSTER_NAME);
        options.addOption(NAMESPACE);
        options.addOption(SET);

        options.getOptions().forEach(opt -> {
            opt.setRequired(true);
        });

        return options;
    }
}
