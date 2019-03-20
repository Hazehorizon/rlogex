package com.hhzone.rlogex.messages;

import com.hhzone.rlogex.filereader.LogType;
import java.util.Date;

public class ReadLogFileMessage extends AbstractMessageBuilder {
    public ReadLogFileMessage(String node, Date date, LogType type) {
        message.put("node", node);
        message.put("type", type.name());
        message.put("date", date.toInstant());
    }
}
