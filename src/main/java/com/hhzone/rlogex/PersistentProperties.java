package com.hhzone.rlogex;

import java.text.SimpleDateFormat;

public interface PersistentProperties {
    String DATE_NODE_LINE_PROPERTY_NAME = "dateNodeLine";
    String LAST_READED_PROPERTY_NAME = "lastReaded";

    SimpleDateFormat DATE_KEY_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
}
