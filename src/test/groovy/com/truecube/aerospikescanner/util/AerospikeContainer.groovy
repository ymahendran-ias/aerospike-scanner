package com.truecube.aerospikescanner.util

import com.aerospike.client.AerospikeClient
import org.rnorth.ducttape.unreliables.Unreliables
import org.testcontainers.containers.ContainerLaunchException
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy

import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

class AerospikeContainer {
    int port
    GenericContainer container

    AerospikeContainer(int port) {
        this.port = port
        this.container = new GenericContainer("aerospike/aerospike-server:4.9.0.37")
                .withExposedPorts(port)

        this.container.waitingFor(new AerospikeWaitStrategy(port, container))
    }

    def getAerospikeContainer() {
        this.container.start()
        return this.container
    }

    static class AerospikeWaitStrategy extends AbstractWaitStrategy {

        int port
        GenericContainer container

        AerospikeWaitStrategy(int port, GenericContainer container) {
            this.port = port
            this.container = container
        }
        @Override
        protected void waitUntilReady() {
            def mappedPort = container.getMappedPort(port)
            def hostname = "localhost"
            try {
                Unreliables.retryUntilSuccess((int) startupTimeout.getSeconds(), TimeUnit.SECONDS, () -> {
                    getRateLimiter().doWhenReady(() -> new AerospikeClient(hostname, mappedPort))
                    return true
                })
            } catch (TimeoutException exception) {
                throw new ContainerLaunchException("Timed out waiting for container port to open (" + hostname + ":" + mappedPort + " should be listening) " + exception.getLocalizedMessage())
            }
        }
    }
}
