package com.hhzone.rlogex.filereader;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;

public class LineContext {
    private final String fileName;
    private final LogType type;
    private final SimpleDateFormat timestampFormat;
    private final Date date;
    private final String node;
    private final long lineNumber;
    private final Matcher matcher;
    private final String line;

    public LineContext(String fileName, LogType type, SimpleDateFormat timestampFormat, Date date, String node, long lineNumber, String line, Matcher matcher) {
        this.fileName = fileName;
        this.type = type;
        this.date = date;
        this.node = node;
        this.lineNumber = lineNumber;
        this.matcher = matcher;
        this.timestampFormat = timestampFormat;
        this.line = line;
    }

    public LogType getType() {
        return type;
    }

    public Date getDate() {
        return date;
    }

    public String getNode() {
        return node;
    }

    public Matcher getMatcher() {
        return matcher;
    }

    public long getLineNumber() {
        return lineNumber;
    }

    public SimpleDateFormat getTimestampFormat() {
        return timestampFormat;
    }

    public String getFileName() {
        return fileName;
    }

    public String getLine() {
        return line;
    }
}
