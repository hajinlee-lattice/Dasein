package com.latticeengines.common.exposed.util;

import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

public class TimeStampConvertUtils {
    private static final Logger log = LoggerFactory.getLogger(TimeStampConvertUtils.class);

    // Mapping from user defined date format to Java 8 date/time library format.
    private static final Map<String, String> userToJavaDateFormatMap = new HashMap<>();
    // Mapping from user defined time format to Java 8 date/time library format.
    private static final Map<String, String> userToJavaTimeFormatMap = new HashMap<>();

    public static final Set<String> SUPPORTED_DATE_FORMAT = new HashSet<>();

    public static final Set<String> SUPPORTED_TIME_FORMAT = new HashSet<>();

    // Set up static mappings from user defined date and time format to Java 8 formats.
    static {
        userToJavaDateFormatMap.put("MM/DD/YYYY", "M/d/yyyy");
        userToJavaDateFormatMap.put("MM-DD-YYYY", "M-d-yyyy");
        userToJavaDateFormatMap.put("MM.DD.YYYY", "M.d.yyyy");
        userToJavaDateFormatMap.put("DD/MM/YYYY", "d/M/yyyy");
        userToJavaDateFormatMap.put("DD-MM-YYYY", "d-M-yyyy");
        userToJavaDateFormatMap.put("DD.MM.YYYY", "d.M.yyyy");
        userToJavaDateFormatMap.put("YYYY/MM/DD", "yyyy/M/d");
        userToJavaDateFormatMap.put("YYYY-MM-DD", "yyyy-M-d");
        userToJavaDateFormatMap.put("YYYY.MM.DD", "yyyy.M.d");
        userToJavaDateFormatMap.put("MM/DD/YY", "M/d/yy");
        userToJavaDateFormatMap.put("MM-DD-YY", "M-d-yy");
        userToJavaDateFormatMap.put("MM.DD.YY", "M.d.yy");
        userToJavaDateFormatMap.put("DD/MM/YY", "d/M/yy");
        userToJavaDateFormatMap.put("DD-MM-YY", "d-M-yy");
        userToJavaDateFormatMap.put("DD.MM.YY", "d.M.yy");

        userToJavaDateFormatMap.put("MMM/DD/YYYY", "MMM/d/yyyy");
        userToJavaDateFormatMap.put("MMM-DD-YYYY", "MMM-d-yyyy");
        userToJavaDateFormatMap.put("MMM.DD.YYYY", "MMM.d.yyyy");
        userToJavaDateFormatMap.put("MMM/DD/YY", "MMM/d/yy");
        userToJavaDateFormatMap.put("MMM-DD-YY", "MMM-d-yy");
        userToJavaDateFormatMap.put("MMM.DD.YY", "MMM.d.yy");
        userToJavaDateFormatMap.put("DD/MMM/YYYY", "d/MMM/yyyy");
        userToJavaDateFormatMap.put("DD-MMM-YYYY", "d-MMM-yyyy");
        userToJavaDateFormatMap.put("DD.MMM.YYYY", "d.MMM.yyyy");
        userToJavaDateFormatMap.put("DD/MMM/YY", "d/MMM/yy");
        userToJavaDateFormatMap.put("DD-MMM-YY", "d-MMM-yy");
        userToJavaDateFormatMap.put("DD.MMM.YY", "d.MMM.yy");
        userToJavaDateFormatMap.put("YYYY/MMM/DD", "yyyy/MMM/d");
        userToJavaDateFormatMap.put("YYYY-MMM-DD", "yyyy-MMM-d");
        userToJavaDateFormatMap.put("YYYY.MMM.DD", "yyyy.MMM.d");

        userToJavaTimeFormatMap.put("00:00:00 12H", "h:m:s a");
        userToJavaTimeFormatMap.put("00-00-00 12H", "h-m-s a");
        userToJavaTimeFormatMap.put("00 00 00 12H", "h m s a");
        userToJavaTimeFormatMap.put("00:00:00 24H", "H:m:s");
        userToJavaTimeFormatMap.put("00-00-00 24H", "H-m-s");
        userToJavaTimeFormatMap.put("00 00 00 24H", "H m s");

        SUPPORTED_DATE_FORMAT.addAll(userToJavaDateFormatMap.keySet());
        SUPPORTED_TIME_FORMAT.addAll(userToJavaTimeFormatMap.keySet());
    }

    // Joda Time date/time formatter for simple original converter.  Only handles dates without times.
    private static final DateTimeFormatter DATE_FORMATTER =
            new DateTimeFormatterBuilder()
                    .append(null, new DateTimeParser[]{
                            DateTimeFormat.forPattern("MM-dd-yyyy").getParser(),
                            DateTimeFormat.forPattern("MM/dd/yy").getParser(),
                            DateTimeFormat.forPattern("MM/dd/yyyy").getParser(),
                            DateTimeFormat.forPattern("yyyy-MM-dd").getParser(),
                            DateTimeFormat.forPattern("yyyy/MM/dd").getParser()})
                    .toFormatter()
                    .withZoneUTC();  // Set default timezone to UTC.

    private static final DateTimeFormatter DATE_FORMATTER_LOCALE =
            new DateTimeFormatterBuilder()
                    .append(null, new DateTimeParser[]{
                            DateTimeFormat.forPattern("dd-MMM-yy").withLocale(Locale.ENGLISH).getParser(),
                            DateTimeFormat.forPattern("dd/MMM/yy").withLocale(Locale.ENGLISH).getParser(),
                            DateTimeFormat.forPattern("dd-MMM-yyyy").withLocale(Locale.ENGLISH).getParser(),
                            DateTimeFormat.forPattern("dd/MMM/yyyy").withLocale(Locale.ENGLISH).getParser()})
                    .toFormatter()
                    .withZoneUTC();

    // Simple method for date conversion which assumes one of five basic date only formats.
    public static long convertToLong(String date) {
        try {
            return DATE_FORMATTER.parseMillis(date);
        } catch (Exception e) {
            /* Possible Errors
            java.lang.IllegalArgumentException
            java.lang.IllegalStateException
             */

            log.warn("Joda Time date/time formatter failed to parse the requested date/time and threw exception:");
            log.warn(e.toString());
            // Uncomment the three lines below if needed for debugging.
            //StringWriter sw = new StringWriter();
            //e.printStackTrace(new PrintWriter(sw));
            //log.warn("Stack Trace is:\n" + sw.toString());
            log.warn("Attempting to use Natty Date Parser to process the date/time.");

            // DO NOT trust natty parser with any month letters in date string!
            if (date.chars().filter(Character::isLetter).count() >= 3) {
                if (date.matches("\\d{2}[-|/][a-zA-Z]{3}[-|/]\\d{2,4}")) {
                    return DATE_FORMATTER_LOCALE.parseMillis(date);
                } else {
                    throw new IllegalArgumentException("Cannot parse date: " + date);
                }
            }
            LogManager.getLogger(Parser.class).setLevel(Level.OFF);
            // Create date/time parser with default timezone UTC.
            Parser parser = new Parser(TimeZone.getTimeZone("UTC"));
            List<DateGroup> groups = parser.parse(date);
            List<Date> dates = groups.get(0).getDates();
            return dates.get(0).getTime();
        }
    }

    // Enhanced date/time conversion which requires user defined date format and time format strings and optional
    // timezone.  If no timezone is provided, UTC is assumed.
    public static long convertToLong(String dateTime, String dateFormatString, String timeFormatString,
                                     String timezone) {
        log.debug(" Date is: " + dateTime + "  Date Format is: " + dateFormatString
                + "  Time Format is: " + timeFormatString + "  Timezone: " + timezone);

        try {
            // Check if a date format string is provided which allows the usage of LocalDateTime from Java 8.
            if (StringUtils.isNotEmpty(dateFormatString)) {
                dateFormatString = dateFormatString.trim();

                // Check if the date format string is in the set of accepted formats.
                if (userToJavaDateFormatMap.containsKey(dateFormatString)) {
                    LocalDateTime localDateTime = null;
                    boolean foundValidTimeFormat = false;
                    log.debug("Found user defined date format: " + dateFormatString);
                    String javaDateFormatStr = userToJavaDateFormatMap.get(dateFormatString);

                    // If the time format string is not empty, make sure it matches an accepted format.
                    if (StringUtils.isNotEmpty(timeFormatString)) {
                        timeFormatString = timeFormatString.trim();

                        if (userToJavaTimeFormatMap.containsKey(timeFormatString)) {
                            foundValidTimeFormat = true;
                            log.debug(" Found user defined time format: " + timeFormatString);
                            String javaTimeFormatStr = userToJavaTimeFormatMap.get(timeFormatString);
                            log.debug(" Java date/time format string is: " + javaDateFormatStr + " "
                                    + javaTimeFormatStr);

                            // Convert to uppercase in case AM/PM is lowercase which Java can't handle.
                            dateTime = dateTime.replaceAll("([aA])([mM])", "AM")
                                    .replaceAll("([pP])([mM])", "PM");
                            // Parse the provided date/time value using a DateTimeFormatter with combined date and time
                            // components.
                            localDateTime = LocalDateTime.parse(dateTime,
                                    java.time.format.DateTimeFormatter.ofPattern(javaDateFormatStr + " "
                                            + javaTimeFormatStr));
                        } else {
                            // If the time format string is not supported, log an error since the pattern string is not
                            // valid.
                            log.error("User provided time format could not be processed: " + timeFormatString);
                            // TODO(jwinter): Consider throwing an error for unrecognized time format pattern from user.
                        }
                    } else {
                        // If a time format string was not provided, log that and use only the date format.
                        log.info("Time format string was empty.  Using on date format.");
                    }

                    // If a time format was not provided or not acceptable, use the date format only and assume the
                    // time is the start of the day.
                    if (!foundValidTimeFormat) {
                        log.debug(" Java date only format string is: " + javaDateFormatStr);
                        // Parse the date value provided using DateTimeFormatter with only a date component.
                        localDateTime = LocalDate.parse(dateTime,
                                java.time.format.DateTimeFormatter.ofPattern(javaDateFormatStr)).atStartOfDay();
                    }


                    // Process timezone.
                    ZoneId zoneId = ZoneId.of("UTC");
                    if (StringUtils.isNotEmpty(timezone)) {
                        zoneId = TimeZone.getTimeZone(timezone).toZoneId();
                        log.debug(" Using zone ID " + zoneId.getId());
                    }

                    long timestamp = localDateTime.atZone(zoneId).toInstant().toEpochMilli();
                    log.debug(" New epoch is: " + timestamp);
                    return timestamp;
                } else {
                    // If the date string is not supported, throw an error since the pattern string is not valid.
                    log.error("User provided data format could not be processed: " + dateFormatString);
                    log.error("Defaulting to using original convertToLong(date)");
                    // TODO(jwinter): Consider throwing an error for unrecognized date format pattern from user.
                    return convertToLong(dateTime);

                }
            } else {
                log.error("User provided date format string is empty, using original convertToLong(date)");
                return convertToLong(dateTime);
            }
        } catch (Exception e) {
            /* Possible Errors
            java.time.format.DateTimeParseException:
            java.lang.IllegalArgumentException
            java.lang.IllegalStateException
             */

            log.error("Caught Exception thrown: " + e.toString());
            // Uncomment the three lines below if needed for debugging.
            //StringWriter sw = new StringWriter();
            //e.printStackTrace(new PrintWriter(sw));
            //log.error("Stack Trace is:\n" + sw.toString());

            log.error("Using original convertToLong(date)");
            return convertToLong(dateTime);
        }
    }

    // Helper method for validating the results of convertToLong().
    public static long computeTimestamp(String dateTime, boolean includesTime, String javaDateTimeFormatStr,
                                        String timezone) {
        if (timezone.isEmpty()) {
            timezone = "UTC";
        }
        LocalDateTime localDateTime;
        if (includesTime) {
            localDateTime = LocalDateTime.parse(dateTime,
                    java.time.format.DateTimeFormatter.ofPattern(javaDateTimeFormatStr));
        } else {
            localDateTime = LocalDate.parse(dateTime,
                    java.time.format.DateTimeFormatter.ofPattern(javaDateTimeFormatStr)).atStartOfDay();
        }
        return localDateTime.atZone(ZoneId.of(timezone)).toInstant().toEpochMilli();
    }

    public static String convertToDate(long timeStamp){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        // Use UTC timezone for conversions.
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(new Date(timeStamp));
    }

    public static String[] getAvailableTimeZoneIDs() {
        return TimeZone.getAvailableIDs();
    }
}
