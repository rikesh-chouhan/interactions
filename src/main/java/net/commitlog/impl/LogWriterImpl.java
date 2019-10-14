package net.commitlog.impl;

import net.commitlog.model.LogWriter;

import java.io.IOException;
import java.io.PrintWriter;

public class LogWriterImpl implements LogWriter {

    private PrintWriter printWriter;

    LogWriterImpl(PrintWriter writeToThis) throws IOException {
        printWriter = writeToThis;
    }

    @Override
    public synchronized void writeToLog(String name, int counter, String data) {
        if (printWriter == null) {
            throw new IllegalStateException("Log writing is done, cannot accept anymore data");
        }

        if (data == null || name == null || name.trim().length() == 0) {
            // don't write to log for unknown name or when there is no data
            return;
        }
        printWriter.printf("%s: %d: %s\n", name, counter, data);
    }

}
