package com.ias.aerospikescanner.util;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StatusUpdate {

    public static String getClusterKeysPath(String workingDir, String namespace, String setName, String clusterName) {
        return workingDir + "/" + namespace + "/" + setName + "/" + clusterName + "/clusterKeys";
    }

    public static String getKeysOutputDirectory(String workingDir, String namespace, String setName) {
        return workingDir + "/" + namespace + "/" + setName + "/missingKeys/";
    }
}
