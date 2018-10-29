package com.latticeengines.common.exposed.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;

public class TimeStampConvertUtils {

    public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    private static final DateTimeFormatter DATE_FORMATTER =
            new DateTimeFormatterBuilder()
                    .append(null, new DateTimeParser[]{
                            DateTimeFormat.forPattern("MM-dd-yyyy").getParser(),
                            DateTimeFormat.forPattern("MM/dd/yyyy").getParser(),
                            DateTimeFormat.forPattern("yyyy-MM-dd").getParser(),
                            DateTimeFormat.forPattern("yyyy/MM/dd").getParser()})
                    .toFormatter();

    public static long convertToLong(String date) {
        try {
            return DATE_FORMATTER.parseLocalDate(date).toDate().getTime();
        } catch (Exception e) {
            LogManager.getLogger(Parser.class).setLevel(Level.OFF);
            Parser parser = new Parser();
            List<DateGroup> groups = parser.parse(date);
            List<Date> dates = groups.get(0).getDates();
            return dates.get(0).getTime();
        }
    }

    public static long convertToLong(String date, String patternString) {
        try {
            if (StringUtils.isNotEmpty(patternString)) {
                DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                        .append(null, new DateTimeParser[]{ DateTimeFormat.forPattern(patternString).getParser()})
                        .toFormatter();

                return formatter.parseLocalDate(date).toDate().getTime();
            } else {
                return convertToLong(date);
            }
        } catch (Exception e) {
            LogManager.getLogger(Parser.class).setLevel(Level.OFF);
            Parser parser = new Parser();
            List<DateGroup> groups = parser.parse(date);
            List<Date> dates = groups.get(0).getDates();
            return dates.get(0).getTime();
        }
    }

    public static String convertToDate(long timeStamp){
        SimpleDateFormat sim = new SimpleDateFormat("yyyy-MM-dd");
        String date = sim.format(new Date(timeStamp));
        return date;
    }
}
