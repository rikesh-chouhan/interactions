package net.commitlog.model;

/**
 * A log writer that writes to stream provided to it
 */
public interface LogWriter {
    void writeToLog(String name, int counter, String data);
}
