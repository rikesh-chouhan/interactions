package net.commitlog.impl;

import net.commitlog.model.Reader;

public class ReaderBuilder {

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        String name;

        private Builder() {}

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Reader build() {
            return new ReaderImpl(this.name);
        }
    }

    private static class ReaderImpl implements Reader {

        int counter = 0;
        String name;

        private ReaderImpl(String readerName) {
            name = readerName;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public int linesRead() {
            return counter;
        }

        @Override
        public void consumeLine(String data) {
            System.out.printf("%s: %s\n", name, data);
            counter++;
        }
    }
}
