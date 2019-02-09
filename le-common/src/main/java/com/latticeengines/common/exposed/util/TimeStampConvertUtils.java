package com.latticeengines.common.exposed.util;

import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.joda.time.DateTime;
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
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

public class TimeStampConvertUtils {
    private static final Logger log = LoggerFactory.getLogger(TimeStampConvertUtils.class);

    // linked hash map to use inserted order as priority at field mapping phase
    // Mapping from user exposed date format to Java 8 date/time library format.
    private static final Map<String, String> userToJavaDateFormatMap = new LinkedHashMap<>();
    // Mapping from user exposed time format to Java 8 date/time library format.
    private static final Map<String, String> userToJavaTimeFormatMap = new LinkedHashMap<>();
    // Mapping from user exposed time zone format to Java 8 supported time zones.
    private static final Map<String, String> userToJavaTimeZoneMap = new LinkedHashMap<>();

    // Lists of supported user date and time formats and time zones.
    public static final List<String> SUPPORTED_USER_DATE_FORMATS = new ArrayList<>();
    public static final List<String> SUPPORTED_USER_TIME_FORMATS = new ArrayList<>();
    public static final List<String> SUPPORTED_USER_TIME_ZONES = new ArrayList<>();

    // List of supported java time zones.
    public static final List<String> SUPPORTED_JAVA_TIME_ZONES = new ArrayList<>();

    // List of all supported java date + time and date only formats.
    public static final List<String> SUPPORTED_JAVA_DATE_TIME_FORMATS = new ArrayList<>();

    // joda Time date/time formatter
    public static final DateTimeFormatter DATE_TIME_FORMATTER;

    // represent all legal date time format in system which generated according
    // to date format and time format

    public static final List<DateTimeFormatter> SUPPORTED_DATE_TIME_FORMATTERS = new ArrayList<>();

    // Set up static mappings from user exposed date and time format to Java 8 formats.
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
    }

    // Set up static mappings for user exposed time zone format to Java 8 format.
    static {
        userToJavaTimeZoneMap.put("UTC-12    Military/Yankee",                           "Etc/GMT+12");
        userToJavaTimeZoneMap.put("UTC-11    Pacific/Pago Pago",                         "Pacific/Pago_Pago");
        userToJavaTimeZoneMap.put("UTC-10    Pacific/Honolulu",                          "Pacific/Honolulu");
        userToJavaTimeZoneMap.put("UTC-9:30  Pacific/Marquesas",                         "Pacific/Marquesas");
        userToJavaTimeZoneMap.put("UTC-9     America/Anchorage",                         "America/Anchorage");
        userToJavaTimeZoneMap.put("UTC-8     America/Los Angeles, America/Vancouver",    "America/Los_Angeles");
        userToJavaTimeZoneMap.put("UTC-7     America/Phoenix, America/Calgary",          "America/Phoenix");
        userToJavaTimeZoneMap.put("UTC-6     America/Chicago, America/Mexico City",      "America/Chicago");
        userToJavaTimeZoneMap.put("UTC-5     America/New York, America/Lima",            "America/New_York");
        userToJavaTimeZoneMap.put("UTC-4     America/Halifax, America/Santiago",         "America/Halifax");
        userToJavaTimeZoneMap.put("UTC-3:30  America/St John's",                         "America/St_Johns");
        userToJavaTimeZoneMap.put("UTC-3     America/Sao Paulo, America/Buenos Aires",   "America/Sao_Paulo");
        userToJavaTimeZoneMap.put("UTC-2     Brazil/de Noronha, Atlantic/South Georgia", "Brazil/DeNoronha");
        userToJavaTimeZoneMap.put("UTC-1     Atlantic/Cape Verde, Atlantic/Azores",      "Atlantic/Cape_Verde");
        userToJavaTimeZoneMap.put("UTC",                                                 "UTC");
        userToJavaTimeZoneMap.put("UTC       Europe/London, Africa/Accra",               "Europe/London");
        userToJavaTimeZoneMap.put("UTC+1     Europe/Berlin, Africa/Lagos",               "Europe/Berlin");
        userToJavaTimeZoneMap.put("UTC+2     Europe/Kiev, Africa/Cairo",                 "Europe/Kiev");
        userToJavaTimeZoneMap.put("UTC+3     Europe/Moscow, Asia/Istanbul",              "Europe/Moscow");
        userToJavaTimeZoneMap.put("UTC+3:30  Asia/Tehran",                               "Asia/Tehran");
        userToJavaTimeZoneMap.put("UTC+4     Asia/Dubai, Asia/Baku",                     "Asia/Dubai");
        userToJavaTimeZoneMap.put("UTC+4:30  Asia/Kabul",                                "Asia/Kabul");
        userToJavaTimeZoneMap.put("UTC+5     Asia/Karachi, Asia/Tashkent",               "Asia/Karachi");
        userToJavaTimeZoneMap.put("UTC+5:30  Asia/Kolkata, Asia/Colombo",                "Asia/Kolkata");
        userToJavaTimeZoneMap.put("UTC+5:45  Asia/Kathmandu",                            "Asia/Kathmandu");
        userToJavaTimeZoneMap.put("UTC+6     Asia/Dhaka, Asia/Almaty",                   "Asia/Dhaka");
        userToJavaTimeZoneMap.put("UTC+6:30  Asia/Yangon, Indian/Cocos",                 "Asia/Yangon");
        userToJavaTimeZoneMap.put("UTC+7     Asia/Jakarta, Asia/Bangkok",                "Asia/Jakarta");
        userToJavaTimeZoneMap.put("UTC+8     Asia/Shanghai, Australia/Perth",            "Asia/Shanghai");
        userToJavaTimeZoneMap.put("UTC+8:45  Australia/Eucla",                           "Australia/Eucla");
        userToJavaTimeZoneMap.put("UTC+9     Asia/Tokyo, Asia/Seoul",                    "Asia/Tokyo");
        userToJavaTimeZoneMap.put("UTC+9:30  Australia/Adelaide",                        "Australia/Adelaide");
        userToJavaTimeZoneMap.put("UTC+10    Australia/Sydney, Pacific/Guam",            "Australia/Sydney");
        userToJavaTimeZoneMap.put("UTC+10:30 Australia/Lord Howe",                       "Australia/Lord_Howe");
        userToJavaTimeZoneMap.put("UTC+11    Pacific/Noumea, Asia/Sakhalin",             "Pacific/Noumea");
        userToJavaTimeZoneMap.put("UTC+12    Pacific/Auckland, Asia/Kamchatka",          "Pacific/Auckland");
        userToJavaTimeZoneMap.put("UTC+12:45 Pacific/Chatham",                           "Pacific/Chatham");
        userToJavaTimeZoneMap.put("UTC+13    Pacific/Apia, Pacific/Tongatapu",           "Pacific/Apia");
        userToJavaTimeZoneMap.put("UTC+14    Pacific/Kiritimati",                        "Pacific/Kiritimati");
    }

    // Set up format data structures for autodetection of formats during user data import workflow.
    static {
        // Set up lists of user supported date and time formats and timezones.
        SUPPORTED_USER_DATE_FORMATS.addAll(userToJavaDateFormatMap.keySet());
        SUPPORTED_USER_TIME_FORMATS.addAll(userToJavaTimeFormatMap.keySet());
        SUPPORTED_USER_TIME_ZONES.addAll(userToJavaTimeZoneMap.keySet());

        SUPPORTED_JAVA_TIME_ZONES.addAll(userToJavaTimeZoneMap.values());

        // Construct all supported java date + time formats (included date only formats).
        for (String dateFormat : userToJavaDateFormatMap.values()) {
            for (String timeFormat : userToJavaTimeFormatMap.values()) {
                SUPPORTED_JAVA_DATE_TIME_FORMATS.add(String.format("%s %s", dateFormat, timeFormat));
            }
        }
        SUPPORTED_JAVA_DATE_TIME_FORMATS.addAll(userToJavaDateFormatMap.values());

        DateTimeParser[] parsers = new DateTimeParser[SUPPORTED_JAVA_DATE_TIME_FORMATS.size()];
        int index = 0;
        for (String format : TimeStampConvertUtils.SUPPORTED_JAVA_DATE_TIME_FORMATS) {
            parsers[index++] = DateTimeFormat.forPattern(format.trim()).getParser();
            SUPPORTED_DATE_TIME_FORMATTERS.add(DateTimeFormat.forPattern(format.trim()));
        }
        DATE_TIME_FORMATTER = new DateTimeFormatterBuilder().append(null, parsers).toFormatter();

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
                    .withZoneUTC();  // Set default time zone to UTC.

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
            // Create date/time parser with default time zone UTC.
            Parser parser = new Parser(TimeZone.getTimeZone("UTC"));
            List<DateGroup> groups = parser.parse(date);
            List<Date> dates = groups.get(0).getDates();
            return dates.get(0).getTime();
        }
    }

    // Enhanced date/time conversion which requires user defined date format and time format strings and optional
    // time zone.  If no time zone is provided, UTC is assumed.
    public static long convertToLong(String dateTime, String userDateFormatStr, String userTimeFormatStr,
                                     String userTimeZoneStr) {
        log.info("Date is: " + dateTime + "  Date Format is: " + userDateFormatStr
                + "  Time Format is: " + userTimeFormatStr + "  Time Zone is: " + userTimeZoneStr);

        // Remove excessive whitespace from the date/time value.  First trim the beginning and end.
        dateTime = dateTime.trim();
        // Now turn any whitespace larger than one space into exactly one space.
        dateTime = dateTime.replaceFirst("(\\s\\s+)", " ");

        try {
            // Check if a date format string is provided which allows the usage of LocalDateTime from Java 8.
            if (StringUtils.isNotEmpty(userDateFormatStr)) {
                userDateFormatStr = userDateFormatStr.trim();

                // Check if the date format string is in the set of accepted formats.
                if (userToJavaDateFormatMap.containsKey(userDateFormatStr)) {
                    LocalDateTime localDateTime = null;
                    boolean foundValidTimeFormat = false;
                    log.info("Found user defined date format: " + userDateFormatStr);
                    String javaDateFormatStr = userToJavaDateFormatMap.get(userDateFormatStr);
                    String javaFormatUsed = javaDateFormatStr;

                    // If the time format string is not empty, make sure it matches an accepted format.
                    if (StringUtils.isNotEmpty(userTimeFormatStr)) {
                        userTimeFormatStr = userTimeFormatStr.trim();

                        if (userToJavaTimeFormatMap.containsKey(userTimeFormatStr)) {
                            log.info("Found user defined time format: " + userTimeFormatStr);
                            String javaTimeFormatStr = userToJavaTimeFormatMap.get(userTimeFormatStr);

                            log.info("Java date/time format string is: " + javaDateFormatStr + " "
                                    + javaTimeFormatStr);

                            // Convert to uppercase in case AM/PM is lowercase which Java can't handle.
                            dateTime = dateTime.replaceAll("([aA])([mM])", "AM")
                                    .replaceAll("([pP])([mM])", "PM");
                            // Parse the provided date/time value using a DateTimeFormatter with combined date and time
                            // components.
                            try {
                                localDateTime = LocalDateTime.parse(dateTime,
                                        java.time.format.DateTimeFormatter.ofPattern(javaDateFormatStr + " "
                                                + javaTimeFormatStr));
                                javaFormatUsed += " " + javaTimeFormatStr;
                            } catch (DateTimeParseException e) {
                                // When parsing date and time doesn't work, try just parsing out a date from the value
                                // as a backup plan.  Strip off extra characters from the date/time which represent the
                                // unparsable time component of the date/time.  Note that this will not work if there is
                                // white space inside the date format, but this is just a heuristic backup plan.
                                log.warn("Could not parse date/time from: " + dateTime
                                        + ".  Trying to strip time component and parse only date");
                                String dateWithTimeStripped = dateTime.replaceFirst("(\\s+\\S+)", "");
                                log.warn("Date value after stripping trailing characters: " + dateWithTimeStripped);
                                try {
                                    localDateTime = LocalDate.parse(dateWithTimeStripped,
                                            java.time.format.DateTimeFormatter.ofPattern(javaDateFormatStr))
                                            .atStartOfDay();
                                } catch (DateTimeParseException e2) {
                                    throw new IllegalArgumentException(
                                            "Date/time value (" + dateTime + ") could not be parsed by format string: " +
                                                    userDateFormatStr + " " + userTimeFormatStr + "\nException was: " +
                                                    e.toString());
                                }
                            }
                        } else {
                            // If the time format string is not supported, log an error since the pattern string is not
                            // valid.
                            log.error("User provided time format is not supported: " + userTimeFormatStr);
                            throw new IllegalArgumentException("User provided time format is not supported: "
                                    + userTimeFormatStr);
                        }
                    } else {
                        // If a time format was not provided, use the date format only and assume the time is the start
                        // of the day.
                        log.info("Time format string was empty.  Using on date format.");
                        log.info("Java date only format string is: " + javaDateFormatStr);
                        // Parse the date value provided using DateTimeFormatter with only a date component.
                        try {
                            localDateTime = LocalDate.parse(dateTime,
                                    java.time.format.DateTimeFormatter.ofPattern(javaDateFormatStr)).atStartOfDay();
                        } catch (DateTimeParseException e) {
                            // When parsing a date doesn't work, try cutting off extra characters from the date/time
                            // string which might represent a time.  Note that this will not work if there is white
                            // space inside the date format, but this is just a heuristic backup plan.
                            log.warn("Could not parse date from: " + dateTime + ".  Trying to strip time component");
                            String dateWithTimeStripped = dateTime.replaceFirst("(\\s+\\S+)", "");
                            log.warn("Date value after stripping trailing characters: " + dateWithTimeStripped);
                            try {
                                localDateTime = LocalDate.parse(dateWithTimeStripped,
                                        java.time.format.DateTimeFormatter.ofPattern(javaDateFormatStr)).atStartOfDay();
                            } catch (DateTimeParseException e2) {
                                throw new IllegalArgumentException("Date value (" + dateTime +
                                        ") could not be parsed by " + "format string: " + userDateFormatStr +
                                        "\nException was: " + e.toString());
                            }
                        }
                    }
                    log.info("LocalDateTime is: " + localDateTime.format(
                            java.time.format.DateTimeFormatter.ofPattern(javaFormatUsed)));

                    // Now process the user provided time zone string, if provided.
                    ZoneId zoneId;
                    if (StringUtils.isNotEmpty(userTimeZoneStr)) {
                        userTimeZoneStr = userTimeZoneStr.trim();

                        if (userToJavaTimeZoneMap.containsKey(userTimeZoneStr)) {
                            log.info("Found user defined time zone: " + userTimeZoneStr);
                            String javaTimeZoneStr = userToJavaTimeZoneMap.get(userTimeZoneStr);
                            log.info("Java time zone string is: " + javaTimeZoneStr);
                            zoneId = TimeZone.getTimeZone(javaTimeZoneStr).toZoneId();
                        } else {
                            log.error("User provided time zone is not supported: " + userTimeZoneStr);
                            throw new IllegalArgumentException("User provided time zone is not supported: "
                                    + userTimeZoneStr);
                        }
                        log.info("Using user provided time zone: " + zoneId.getId());
                    } else {
                        log.info("User provided time zone string is empty, using UTC");
                        zoneId = ZoneId.of("UTC");
                    }

                    // Convert the LocalDateTime to a millisecond timestamp according the the provided (or UTC default)
                    // timezone.
                    long timestamp = localDateTime.atZone(zoneId).toInstant().toEpochMilli();
                    log.info("Millisecond timestamp is: " + timestamp);

                    return timestamp;
                } else {
                    // If the date string is not supported, throw an error since the pattern string is not valid.
                    log.error("User provided data format is not supported: " + userDateFormatStr);
                    throw new IllegalArgumentException("User provided data format is not supported: " +
                            userDateFormatStr);
                }
            } else {
                // For backwards compatibility, if no user date format string was provided, use the original function
                // for converting date string to timestamp.  If no date format was provided, this function is either
                // being called in a place that assumes the old functionality or is being called on a date value
                // that was generated before date/time formats were stored.  In either case, the original behavior is
                // likely desired.
                log.warn("User provided date format string is empty, using original convertToLong(date)");
                return convertToLong(dateTime);
            }
        } catch (Exception e) {
            /* Possible Errors
            java.lang.IllegalArgumentException
            java.lang.IllegalStateException
             */

            log.error("Caught Exception thrown: " + e.toString());
            // Uncomment the three lines below if needed for debugging.
            //StringWriter sw = new StringWriter();
            //e.printStackTrace(new PrintWriter(sw));
            //log.error("Stack Trace is:\n" + sw.toString());
            throw e;
        }
    }

    // Helper method for validating the results of convertToLong().
    public static long computeTimestamp(String dateTime, boolean includesTime, String javaDateTimeFormatStr,
                                        String timeZoneString) {
        if (timeZoneString.isEmpty()) {
            timeZoneString = "UTC";
        }
        LocalDateTime localDateTime;
        if (includesTime) {
            localDateTime = LocalDateTime.parse(dateTime,
                    java.time.format.DateTimeFormatter.ofPattern(javaDateTimeFormatStr));
        } else {
            localDateTime = LocalDate.parse(dateTime,
                    java.time.format.DateTimeFormatter.ofPattern(javaDateTimeFormatStr)).atStartOfDay();
        }
        return localDateTime.atZone(ZoneId.of(timeZoneString)).toInstant().toEpochMilli();
    }

    public static String convertToDate(long timeStamp){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        // Use UTC time zone for conversions.
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(new Date(timeStamp));
    }

    public static String[] getAvailableTimeZoneIDs() {
        return TimeZone.getAvailableIDs();
    }

    public static Set<String> getAvailableZoneIds() {
        return ZoneId.getAvailableZoneIds();
    }

    /*
     * write this method due to bug in SUPPORTED_DATE_TIME_FORMATER, I put 210
     * formats to SUPPORTED_DATE_TIME_FORMATER,while it can't parse value whose
     * format locate in last 30 positions
     */
    public static DateTime parseDateTime(String value) {
        DateTime dateTime = null;
        for (DateTimeFormatter formatter : SUPPORTED_DATE_TIME_FORMATTERS) {
            try {
                dateTime = formatter.parseDateTime(value);
            } catch (Exception e) {
            }
            if (dateTime != null) {
                break;
            }
        }
        return dateTime;
    }
}
