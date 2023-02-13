package com.ias.aerospikescanner;

import com.ias.aerospikescanner.util.StatusUpdate;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@AllArgsConstructor
@Slf4j
public class KeysComparor {
    private final String kosherKeysDir;
    private final String culpritKeysDir;
    private final String outputDir;

    public KeysComparor(String workingDir, String namespace, String setName, String kosherClusterName, String culpritClusterName) {
        this.kosherKeysDir = StatusUpdate.getClusterKeysPath(workingDir, namespace, setName, kosherClusterName);
        this.culpritKeysDir = StatusUpdate.getClusterKeysPath(workingDir, namespace, setName, culpritClusterName);
        this.outputDir = StatusUpdate.getKeysOutputDirectory(workingDir, namespace, setName);
    }

    public void process() throws Exception {
        log.info("Staring to compare keys from both cluster!");
        Map<String, String> kosherData = loadFilesToMap(kosherKeysDir);
        Map<String, String> culpritData = loadFilesToMap(culpritKeysDir);

        log.info("\t\t Number of keys found in Kosher cluster: " + kosherData.size());
        log.info("\t\t Number of keys found in Culprit cluster: " + culpritData.size());

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

        log.info("\t\t Number of keys that were missing from Kosher Cluster: " + missingCount.get());
        log.info("\t\t Number of keys that were found in Kosher Cluster: " + matchingCount.get());

        kosherData.clear();
        culpritData.clear();
        writeMissingKeysToFile(missingData);

    }

    private void writeMissingKeysToFile(Map<String, String> missingData) throws Exception {
        Files.createDirectories(Paths.get(outputDir));

        File dir = new File(culpritKeysDir);
        File[] directoryListing = dir.listFiles();

        AtomicInteger count = new AtomicInteger(0);
        final KeyContainer prevKeyMissingCache = new KeyContainer();
        final KeyContainer prevKeyPresentCache = new KeyContainer();
        if (directoryListing != null) {
            for (File culpritFile : directoryListing) {
                File file = new File(outputDir, "missingKeysFrom-"+culpritFile.getName());
                if (!file.exists()) {
                    Files.createFile(file.toPath());
                }
                FileOutputStream outputStream = new FileOutputStream(file.toPath().toString());
                Stream<String> lines = Files.lines(culpritFile.toPath());
                lines.forEach(line -> {
                    count.incrementAndGet();
                    String[] parts = getParts(line);
                    String curKey = parts[0];
                    boolean writeToFile = false;
                    if(curKey.equals(prevKeyMissingCache.key)) {
                        writeToFile = true;
                    } else if(curKey.equals(prevKeyPresentCache.key)) {
                        writeToFile = false;
                    } else {
                        if(missingData.containsKey(curKey)) {
                            prevKeyMissingCache.key = curKey;
                            writeToFile = true;
                        } else {
                            prevKeyPresentCache.key = curKey;
                            writeToFile = false;
                        }
                    }
                    if(writeToFile) {
                        String output = line + "\n";
                        try {
                            outputStream.write(output.getBytes());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    if(count.get() % 1_000_000 == 0) {
                        log.info("\t\t Number of records looked up to fetch missing keys: " + count.get());
                    }
                });
                lines.close();
                if(null != outputStream) {
                    outputStream.close();
                }
            }
        }

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

    private static class KeyContainer {
        public String key = "";
    }
}
