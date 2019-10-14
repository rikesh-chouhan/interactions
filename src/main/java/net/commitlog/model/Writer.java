package net.commitlog.model;

public interface Writer {
    String name();
    int linesWritten();
    LogWriter logWriter();
    void runWriter();
}
