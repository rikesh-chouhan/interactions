package net.commitlog.impl;

import net.commitlog.model.LogReader;
import net.commitlog.model.LogWriter;
import net.commitlog.model.Reader;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class LogFactory {

    private String logFile;
    private LogWriterImpl logger;
    private LogReaderImpl logReader;
    private BufferedReader toReadFrom;
    private PrintWriter logWriter;
    private long waitTimeForReading;
    private long fileExistsSleepTime;

    public LogFactory(String logFileName, long sleepTime, long waitTime) {
        logFile = logFileName;
        fileExistsSleepTime = sleepTime;
        waitTimeForReading = waitTime;
    }

    public synchronized LogWriter provideLogWriter(boolean appendToFile) throws IOException {
        if (logger == null) {
            logWriter = new PrintWriter(new BufferedWriter(new FileWriter(logFile, appendToFile)));
            logger = new LogWriterImpl(logWriter);
        }
        return logger;
    }

    public synchronized LogReader provideLogReader(ExecutorService executorService, List<Reader> readerList) {
        if (logReader == null) {
            logReader = new LogReaderImpl();
            for (Reader reader: readerList) {
                logReader.addReader(reader);
            }
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    System.out.println("Checking if file is available for reading");
                    boolean keepRunning = true;
                    long timeToWait = System.currentTimeMillis();
                    while(keepRunning) {
                        long currentTime = System.currentTimeMillis();
                        if (currentTime > (timeToWait+waitTimeForReading)) {
                            throw new RuntimeException("Waited for log file from: {} "+new Date(timeToWait)
                                    + "and didn't find it up till now: "+new Date(currentTime));
                        }
                        File checkForExists = new File(logFile);
                        if (checkForExists.exists() && checkForExists.isFile()) {
                            try {
                                toReadFrom = new BufferedReader(new FileReader(logFile));
                                logReader.startReading(toReadFrom);
                                keepRunning = false;
                            } catch (FileNotFoundException fne) {
                                // this should not happen, since we determined the file exists before reading
                                throw new RuntimeException(fne);
                            } catch (IOException ioe) {
                                throw new RuntimeException(ioe);
                            } finally {
                                stopReading();
                            }
                        } else {
                            try {
                                Thread.sleep(fileExistsSleepTime);
                            } catch (InterruptedException ie) {
                                // wait for elapsed time handler to throw the exception
                                ie.printStackTrace();
                            }
                        }
                    }
                }
            });
        }
        return logReader;
    }

    public synchronized void stopReading() {
        try {
            logReader = null;
            if (toReadFrom != null) {
                toReadFrom.close();
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
            toReadFrom = null;
        }
    }

    public synchronized void stopWriting() {
        if (logWriter != null) {
            logWriter.flush();
            logWriter.close();
            logWriter = null;
            logger = null;
        }
    }

}
