package com.truecube.aerospikescanner.util;

import java.util.concurrent.atomic.AtomicLong;

public class PassRate {
    private final AtomicLong passes;
    private final AtomicLong failures;

    public PassRate() {
        passes = new AtomicLong();
        failures = new AtomicLong();
    }

    public PassRate add(boolean isPass) {
        long l = isPass ? passes.incrementAndGet() : failures.incrementAndGet();
        return this;
    }

    public PassRate addAll(PassRate passRate) {
        passes.addAndGet(passRate.getPasses().get());
        failures.addAndGet(passRate.getFailures().get());
        return this;
    }

    public AtomicLong getPasses() {
        return passes;
    }

    public AtomicLong getFailures() {
        return failures;
    }

    @Override
    public String toString() {
        long totalPasses = passes.get();
        long totalFailures = failures.get();
        long totalReads = totalFailures + totalPasses;
        return ((totalPasses * 100) / totalReads) + "." + ((totalPasses * 100) % totalReads) + "% passed out of " + totalReads + " requests";
    }
}
