package com.truecube.aerospikescanner

import com.aerospike.client.AerospikeClient
import com.aerospike.client.Bin
import com.aerospike.client.Key
import com.aerospike.client.command.Buffer
import com.aerospike.client.policy.ClientPolicy
import com.aerospike.client.policy.WritePolicy
import com.truecube.aerospikescanner.util.AerospikeContainer
import org.testcontainers.containers.GenericContainer
import spock.lang.Specification

class ScannerJobFullIntegrationTest extends Specification {

    private final int port = 3000
    private final GenericContainer culpritContainer = new AerospikeContainer(port).getAerospikeContainer()
    private final GenericContainer kosherContainer = new AerospikeContainer(port).getAerospikeContainer()
    private final String setName = "cat_scores"
    private final String namespace = "test"
    private final String workingDir = "/tmp"
    private final String kosherClusterName = "kosher"
    private final String culpritClusterName = "culprit"

    def "test right missing keys identified when two clusters with differing keys exist"() {
        given:
        //Two Aerospike clusters
        AerospikeClient kosherClient = getKosherAerospikeClient()
        AerospikeClient culpritClient = getCulpritAerospikeClient()

        and:
        //Frew keys that are in both clusters
        writeToAerospike(
                [kosherClient, culpritClient], //clusters
                [
                        new Key(namespace, setName, "URL1"),
                        new Key(namespace, setName, "URL2"),
                        new Key(namespace, setName, "URL3"),
                        new Key(namespace, setName, "URL4")
                ])

        //and few keys that's just in one cluster
        def keysInOneCluster = [
                new Key(namespace, setName, "URL5"),
                new Key(namespace, setName, "URL6"),
                new Key(namespace, setName, "URL7")
        ]
        writeToAerospike([culpritClient], keysInOneCluster)

        when:
        //when the scannerJob is triggered
        ScannerJob.main(prepareArgsForScannerJob())

        then:
        def missingKeys = loadMissingKeysFromFilesIntoList()
        assert(missingKeys.size() == keysInOneCluster.size())
        boolean allMissingKeysFound = true
        keysInOneCluster.forEach(key -> {
            if(allMissingKeysFound) { //if none missing so far, then analyze next key's presence
                allMissingKeysFound = missingKeys.contains(Buffer.bytesToHexString(key.digest))
            }
        })
        assert(allMissingKeysFound)
    }

    AerospikeClient getCulpritAerospikeClient() {
        return new AerospikeClient(new ClientPolicy(), culpritContainer.getHost(), culpritContainer.getMappedPort(port))
    }

    AerospikeClient getKosherAerospikeClient() {
        return new AerospikeClient(new ClientPolicy(), kosherContainer.getHost(), kosherContainer.getMappedPort(port))
    }

    List<String> loadMissingKeysFromFilesIntoList() {
        def mainPath = workingDir+"/"+namespace+"/"+setName
        def culpritFile = ""

        def dir = new File(mainPath+"/"+culpritClusterName+"/clusterKeys/")
        dir.eachFileRecurse (groovy.io.FileType.FILES) { file ->
            culpritFile = file.getName()
        }
        String pathToMissingKeys = mainPath+"/missingKeys/missingKeysFrom-"+culpritFile
        return new File(pathToMissingKeys).readLines()
    }

    def writeToAerospike(List<AerospikeClient> clients, List<Key> keys) {
        clients.forEach(client -> {
            keys.forEach( key -> {
                Bin bin = new Bin("fakeBin", "fakeValue")
                client.put(new WritePolicy(), key, bin)
            })
        })
    }

    def prepareArgsForScannerJob() {
        return new String[]{
            "-ccn", culpritClusterName,
            "-cServer", "localhost",
            "-cPort", culpritContainer.getMappedPort(port)+"",
            "-csUser", "",
            "-csPwd", "",
            "-kcn", kosherClusterName,
            "-kServer", "localhost",
            "-kPort", kosherContainer.getMappedPort(port)+"",
            "-ksUser", "",
            "-ksPwd", "",
            "-namespace", namespace,
            "-kSet", setName,
            "-cSet", setName,
            "-wd", workingDir
        }
    }
}
