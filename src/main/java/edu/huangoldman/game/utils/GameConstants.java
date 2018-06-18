package edu.huangoldman.game.utils;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.TimeZone;

public class GameConstants {
    public static final String TIMESTAMP_ATTRIBUTE = "timestamp_ms";

    public static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormat.forPattern("yyyy-MM-dd HH:MM:ss.SSS")
                .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("GTM+8")));

}
