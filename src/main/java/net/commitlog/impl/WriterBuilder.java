package net.commitlog.impl;

import net.commitlog.model.LogWriter;
import net.commitlog.model.Writer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class WriterBuilder {

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        String name;
        AtomicInteger sharedCounter;
        LogWriter logWriter;
        ExecutorService executorService;

        private Builder() {}

        public Builder withCounter(AtomicInteger counter) {
            this.sharedCounter = counter;
            return this;
        }

        public Builder withWriter(LogWriter writer) {
            this.logWriter = writer;
            return this;
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withExecutor(ExecutorService service) {
            this.executorService = service;
            return this;
        }

        public Writer build() {
            return new WriterImpl(name, sharedCounter, logWriter, executorService);
        }
    }

    private static class WriterImpl implements Writer {

        int linesCounter = 0;
        String name;
        LogWriter logWriter;
        AtomicInteger logCounter;
        ExecutorService executorService;

        @Override
        public int linesWritten() {
            return linesCounter;
        }

        @Override
        public LogWriter logWriter() {
            return logWriter;
        }

        private WriterImpl(String writerName, AtomicInteger counter, LogWriter writer,
                           ExecutorService service) {
            name = writerName;
            logCounter = counter;
            logWriter = writer;
            executorService = service;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public void runWriter() {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    while(true) {
                        try {
                            Thread.sleep(500l);
                        } catch (InterruptedException ie) {
                            ie.printStackTrace();
                            break;
                        }
                        logWriter.writeToLog(name, logCounter.incrementAndGet(),
                                "This data written from Thread: " + Thread.currentThread().getId());
                    }
                }
            });
        }
    }

}
