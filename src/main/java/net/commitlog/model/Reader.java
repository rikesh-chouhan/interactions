package net.commitlog.model;

public interface Reader {
    String name();
    int linesRead();
    void consumeLine(String data);
}
