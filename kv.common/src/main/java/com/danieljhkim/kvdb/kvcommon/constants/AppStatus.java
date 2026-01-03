package com.danieljhkim.kvdb.kvcommon.constants;

public enum AppStatus {
    GREEN("Code Green - All good fam"),
    YELLOW("Code Yellow - Degraded performance"),
    RED("Code Red – Something needs to be done immediately"),
    BLUE("Code Blue - Under maintenance"),
    GRAY("Code Gray - initializing");

    private final String description;

    AppStatus(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    /** Returns true if the app is in a fully operational (GREEN) state. */
    public boolean isHealthy() {
        return this == GREEN;
    }

    /** Returns true if the all the nodes are down (RED) state. */
    public boolean isCritical() {

        return this == RED;
    }

    @Override
    public String toString() {
        return name() + " - " + description;
    }
}
