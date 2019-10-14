package net.commitlog.program;

import net.commitlog.impl.LogFactory;
import net.commitlog.impl.ReaderBuilder;
import net.commitlog.impl.WriterBuilder;
import net.commitlog.model.LogReader;
import net.commitlog.model.LogWriter;
import net.commitlog.model.Reader;
import net.commitlog.model.Writer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class Driver {

    private final static long DEFAULT_SLEEP_TIME = 1000l;
    private final static long DEFAULT_WAIT_TIME = 60000l;

    enum Operation {
        WRITE,
        READ;
    }

    LogFactory theLogHelper;
    Operation operation;
    Properties properties;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("usage: writer|reader property_file_name");
            System.out.println("specify \"WRITE\" as 1st parameter to run the log writer program");
            System.out.println("specify \"READ\" as 1st parameter to run the log reader program");
            System.out.println("specify full path of property_file_name for corresponding program");
        } else {
            String driver = args[0];
            Operation type = Operation.valueOf(driver);
            if (type == Operation.WRITE) {
                driveWriters(args[1]);
            } else if (type == Operation.READ) {
                driveReaders(args[1]);
            } else {
                System.out.println("Invalid 1st entry: " + driver);
            }
        }

    }

    public static void driveWriters(String configFile) {
        Properties properties = loadPropertiesFile(configFile);
        if (properties != null) {
            Driver writer = new Driver(Operation.WRITE, properties);
            try {
                writer.drive();
            } catch (IOException e) {
                e.printStackTrace();
                writer.stop();
            }
        } else {
            System.out.println("Could not load specified Log writer Properties file: " + configFile);
        }
    }

    public static void driveReaders(String configFile) {
        Properties properties = loadPropertiesFile(configFile);
        if (properties != null) {
            Driver reader = new Driver(Operation.READ, properties);
            try {
                reader.drive();
            } catch (IOException e) {
                e.printStackTrace();
                reader.stop();
            }
        } else {
            System.out.println("Could not load specified Log writer Properties file: " + configFile);
        }
    }

    private static Properties loadPropertiesFile(String file) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(file));
        } catch (IOException fne) {
            fne.printStackTrace();
            return null;
        }
        return properties;
    }

    private Driver(Operation type, Properties properties) {
        operation = type;
        this.properties = properties;
        init();
    }

    private void init() {
        String logFileName = properties.getProperty("commitlog","commitlog.txt");
        long sleepTime = Long.parseLong(properties.getProperty("sleepTime", ""+DEFAULT_SLEEP_TIME));
        long totalWaitTime = Long.parseLong(properties.getProperty("waitTime", "" + DEFAULT_WAIT_TIME));
        theLogHelper = new LogFactory(logFileName, sleepTime, totalWaitTime);
    }

    public void drive() throws IOException {
        String names = properties.getProperty("names");
        String[] nameArray = names.split(",");
        if (operation == Operation.WRITE) {
            boolean appendToFile = Boolean.parseBoolean(properties.getProperty("appendToFile", "true"));
            LogWriter logWriter = theLogHelper.provideLogWriter(appendToFile);
            System.out.println("obtained log writer");
            ExecutorService executorService = Executors.newFixedThreadPool(4);
            WriterBuilder.Builder builder = WriterBuilder.builder();
            // if you want to share the counter across all writers
            // builder.withCounter(new AtomicInteger());
            builder.withWriter(logWriter);
            builder.withExecutor(executorService);
            List<Writer> writerList = new ArrayList();
            for (String name: nameArray) {
                int count = Integer.parseInt(properties.getProperty(name,"1"));
                count = Math.abs(count);
                builder.withCounter(new AtomicInteger());
                for (int i=1; i<=count; i++) {
                    builder.withName(name);
                    writerList.add(builder.build());
                }
            }

            if (writerList.size() >0) {
                for (Writer writer : writerList) {
                    writer.runWriter();
                }
                System.out.println("Running writers...");
                // FUTURE todo
                // A timer should be used here to stop the writers after sometime
            } else {
                System.out.println("No writers available to write");
            }
        } else if (operation == Operation.READ) {
            ReaderBuilder.Builder builder = ReaderBuilder.builder();
            List<Reader> readerList = new ArrayList();
            for (String name: nameArray) {
                builder.withName(name);
                readerList.add(builder.build());
            }
            if (readerList.size() > 0) {
                System.out.println("LogReader will attempt to read file");
                long waitTime = 1000l;
                ExecutorService executorService = Executors.newFixedThreadPool(2);
                LogReader logReader = theLogHelper.provideLogReader(executorService, readerList);
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        System.out.println("Waiting for sometime");
                        long counter = 100;
                        while(counter<= waitTime) {
                            try {
                                Thread.sleep(1000l);
                            } catch (InterruptedException ie) {
                                ie.printStackTrace();
                            }
                            counter += 100;
                        }
                    }
                });
                System.out.println("LogReader: "+logReader+" will read if file is available");
            } else {
                System.out.println("Found no reader names, stopping.");
            }
        }
    }

    public void stop() {
        if (operation == Operation.WRITE) {
            theLogHelper.stopWriting();
        } else {
            theLogHelper.stopReading();
        }
    }
}
