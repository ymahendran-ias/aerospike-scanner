package com.ias.aerospikescanner;

import com.ias.aerospikescanner.util.FSUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@AllArgsConstructor
@Slf4j
public class KeysComparor {
    private final String kosherKeysDir;
    private final String culpritKeysDir;
    private final String outputDir;

    public KeysComparor(String workingDir, String namespace, String setName, String kosherClusterName, String culpritClusterName) {
        this.kosherKeysDir = FSUtil.getClusterKeysPath(workingDir, namespace, setName, kosherClusterName);
        this.culpritKeysDir = FSUtil.getClusterKeysPath(workingDir, namespace, setName, culpritClusterName);
        this.outputDir = FSUtil.getKeysOutputDirectory(workingDir, namespace, setName);
    }

    public void process() throws Exception {
        log.info("Staring to compare keys from both cluster!");
        Map<String, String> kosherData = loadFilesToMap(kosherKeysDir);
        Map<String, String> culpritData = loadFilesToMap(culpritKeysDir);

        log.info("\t\t Number of total keys found in Kosher cluster: " + kosherData.size());
        log.info("\t\t Number of total keys found in Culprit cluster: " + culpritData.size());

        AtomicInteger missingCount = new AtomicInteger(0);
        AtomicInteger matchingCount = new AtomicInteger(0);

        Map<String, String> missingData = new TreeMap<>();
        culpritData.keySet().forEach(key -> {
            if(!kosherData.containsKey(key)) {
                missingData.put(key, "");
                missingCount.incrementAndGet();
            } else {
                matchingCount.incrementAndGet();
            }
        });

        log.info("\t\t Number of probable missing keys from Kosher Cluster: " + missingCount.get());
        log.info("\t\t Number of keys found in Kosher Cluster: " + matchingCount.get());

        kosherData.clear();
        culpritData.clear();
        log.info("\t\t Fetch exact missing keys from Kosher Cluster");
        Map<String, Set<String>> missingKeys = fetchMissingKeys(missingData);
        log.info("\t\t Persist exact missing keys from Kosher Cluster");
        writeMissingKeys(missingKeys);
    }

    private Map<String, Set<String>> fetchMissingKeys(Map<String, String> missingData) throws Exception {
        Files.createDirectories(Paths.get(outputDir));
        AtomicInteger count = new AtomicInteger(0);
        File[] files = getFiles();
        Map<String, Set<String>> missingKeys = new HashMap<>();
        if (files != null) {
            for (File culpritFile : files) {
                missingKeys.put(culpritFile.getName(), new HashSet<>());
                Stream<String> lines = Files.lines(culpritFile.toPath());
                lines.forEach(line -> {
                    count.incrementAndGet();
                    String[] parts = getParts(line);
                    String curKey = parts[0];
                    if(missingData.containsKey(curKey)) {
                        String output = line + "\n";
                        missingKeys.get(culpritFile.getName()).add(output);
                    }
                });
            }
        }
        return missingKeys;
    }

    private void writeMissingKeys(Map<String, Set<String>> missingKeys) {
        missingKeys.forEach((sourceFile, keys) -> {
            try {
                File output = createOutputFile(sourceFile);
                FileOutputStream outputStream = new FileOutputStream(output.toPath().toString());
                log.info("\t\t total missing keys found from cluster ("+sourceFile+"): " + keys.size());
                keys.forEach(key -> {
                    try {
                        outputStream.write(key.getBytes());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                outputStream.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Map<String, String> loadFilesToMap(String directory) throws Exception {
        Map<String, String> data = new TreeMap<>();
        File dir = new File(directory);
        File[] directoryListing = dir.listFiles();
        if (directoryListing != null) {
            for (File file : directoryListing) {
                Stream<String> lines = Files.lines(file.toPath());
                AtomicInteger count = new AtomicInteger(0);
                lines.forEach(line -> {
                    count.incrementAndGet();
                    String[] parts = getParts(line);
                    data.putIfAbsent(parts[0], "");
                });
                lines.close();
            }
        } else {
            throw new Exception("problem accessing directory " + directory);
        }
        return data;
    }

    private String[] getParts(String key) {
        String[] parts = new String[3];
        int firstStop = 7;
        int secondStop = 11;
        parts[0] = key.substring(0,firstStop);
        parts[1] = key.substring(firstStop, secondStop);
        parts[2] = key.substring(secondStop);
        return parts;
    }

    private File createOutputFile(String sourceFileName) throws Exception {
        File output = new File(outputDir, "missingKeysFrom-" + sourceFileName);
        if (!output.exists()) {
            Files.createFile(output.toPath());
        }
        return output;
    }

    private File[] getFiles() {
        File dir = new File(culpritKeysDir);
        return dir.listFiles();
    }
}
