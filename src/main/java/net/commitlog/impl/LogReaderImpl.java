package net.commitlog.impl;

import net.commitlog.model.LogReader;
import net.commitlog.model.Reader;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LogReaderImpl implements LogReader {

    private Map<String, Reader> readerMap;
    private static String DEFAULT_LINE = "waiting";

    LogReaderImpl() {
        readerMap = new HashMap();
    }

    @Override
    public void addReader(Reader reader) {
        if (reader != null) {
            readerMap.put(reader.name(), reader);
        }
    }

    synchronized void startReading(BufferedReader toReadFrom) throws IOException {
        String line = DEFAULT_LINE;
        while (line != null) {
            if (line.length() == 0) {
                // do not process this line
            } else {
                processLine(line);
            }
            if (toReadFrom.ready()) {
                line = toReadFrom.readLine();
            } else {
                try {
                    Thread.sleep(1000l);
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }
                line = DEFAULT_LINE;
            }
        }
    }

    private void processLine(@NonNull String line) {
        if (line.equals(DEFAULT_LINE)) {
            return;
        }
        String[] array = line.split(":");
        if (array.length >0) {
            Reader reader = readerMap.get(array[0]);
            String data = "";
            if (array.length>1) {
                // the data including the linesCounter is after the name and the : separator
                data = line.substring(array[0].length()+1);
            }
            if (reader != null && data.length() > 0) {
                reader.consumeLine(data);
            } else if(reader != null) {
                System.out.println("Data for Reader: " + array[0] + " is empty");
            } else {
                System.out.println("Did not find a Reader for: " + array[0] + " for data: "+data);
            }
        }
    }

}
