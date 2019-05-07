package com.latticeengines.common.exposed.util;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;

public class TimeStampConvertUtils {
    private static final Logger log = LoggerFactory.getLogger(TimeStampConvertUtils.class);

    private static final Pattern MONTH = Pattern.compile("([a-zA-Z])([a-zA-Z]{2,})");

    // Regular expression pattern to match date/time formats with ISO 8601 format include "T" between date and time
    // and optional "Z" at the end.
    private static final Pattern TZ_DATE_TIME = Pattern.compile(
            "((\\d{1,4}|[a-zA-Z]{3})[-/.](\\d{1,4}|[a-zA-Z]{3})[-/.]\\d{1,4})[Tt]"
                    + "(\\d{1,2}[-: ]\\d{1,2}([-: ]\\d{1,2})?(\\s+[aApP][mM])?)[Zz]?");

    // Regular expression pattern to match all current date/time formats.
    private static final Pattern DATE_TIME = Pattern.compile(
            "((\\d{1,4}|[a-zA-Z]{3})[-/.](\\d{1,4}|[a-zA-Z]{3})[-/.]\\d{1,4})\\s+"
                    + "(\\d{1,2}[-: ]\\d{1,2}([-: ]\\d{1,2})?(\\s+[aApP][mM])?)");

    // Linked hash maps are used to preserve insertion order as the priority at field mapping phase
    // and to keep generated lists of user supported date formats, time formats, and time zones in order.
    // Mapping from user exposed date format to Java 8 date/time library format.
    private static final Map<String, String> userToJavaDateFormatMap = new LinkedHashMap<>();
    // Mapping from user exposed time format to Java 8 date/time library format.
    private static final Map<String, String> userToJavaTimeFormatMap = new LinkedHashMap<>();
    // Mapping from user exposed time zone format to Java 8 supported time zones.
    private static final Map<String, String> userToJavaTimeZoneMap = new LinkedHashMap<>();

    // Reverse mapping from Java 8 date/time library date format to user exposed format.
    private static final Map<String, String> javaToUserDateFormatMap = new LinkedHashMap<>();
    // Reverse mapping from Java 8 date/time library time format to user exposed format.
    private static final Map<String, String> javaToUserTimeFormatMap = new LinkedHashMap<>();

    // List of all supported java date + time and date only formats.
    public static final List<String> SUPPORTED_JAVA_DATE_TIME_FORMATS = new ArrayList<>();

    // represent all legal date time format in system which generated according
    // to date format and time format
    public static final List<java.time.format.DateTimeFormatter> SUPPORTED_DATE_TIME_FORMATTERS = new ArrayList<>();

    // Set up static mappings from user exposed date and time format to Java 8 formats.
    static {
        userToJavaDateFormatMap.put("MM/DD/YYYY", "M/d/yyyy");
        userToJavaDateFormatMap.put("MM/DD/YY", "M/d/yy");
        userToJavaDateFormatMap.put("DD/MM/YYYY", "d/M/yyyy");
        userToJavaDateFormatMap.put("DD/MM/YY", "d/M/yy");
        userToJavaDateFormatMap.put("YYYY/MM/DD", "yyyy/M/d");

        userToJavaDateFormatMap.put("MM-DD-YYYY", "M-d-yyyy");
        userToJavaDateFormatMap.put("MM-DD-YY", "M-d-yy");
        userToJavaDateFormatMap.put("DD-MM-YYYY", "d-M-yyyy");
        userToJavaDateFormatMap.put("DD-MM-YY", "d-M-yy");
        userToJavaDateFormatMap.put("YYYY-MM-DD", "yyyy-M-d");

        userToJavaDateFormatMap.put("MM.DD.YYYY", "M.d.yyyy");
        userToJavaDateFormatMap.put("MM.DD.YY", "M.d.yy");
        userToJavaDateFormatMap.put("DD.MM.YYYY", "d.M.yyyy");
        userToJavaDateFormatMap.put("DD.MM.YY", "d.M.yy");
        userToJavaDateFormatMap.put("YYYY.MM.DD", "yyyy.M.d");

        userToJavaDateFormatMap.put("MMM/DD/YYYY", "MMM/d/yyyy");
        userToJavaDateFormatMap.put("MMM/DD/YY", "MMM/d/yy");
        userToJavaDateFormatMap.put("DD/MMM/YYYY", "d/MMM/yyyy");
        userToJavaDateFormatMap.put("DD/MMM/YY", "d/MMM/yy");
        userToJavaDateFormatMap.put("YYYY/MMM/DD", "yyyy/MMM/d");

        userToJavaDateFormatMap.put("MMM-DD-YYYY", "MMM-d-yyyy");
        userToJavaDateFormatMap.put("MMM-DD-YY", "MMM-d-yy");
        userToJavaDateFormatMap.put("DD-MMM-YYYY", "d-MMM-yyyy");
        userToJavaDateFormatMap.put("DD-MMM-YY", "d-MMM-yy");
        userToJavaDateFormatMap.put("YYYY-MMM-DD", "yyyy-MMM-d");

        userToJavaDateFormatMap.put("MMM.DD.YYYY", "MMM.d.yyyy");
        userToJavaDateFormatMap.put("MMM.DD.YY", "MMM.d.yy");
        userToJavaDateFormatMap.put("DD.MMM.YYYY", "d.MMM.yyyy");
        userToJavaDateFormatMap.put("DD.MMM.YY", "d.MMM.yy");
        userToJavaDateFormatMap.put("YYYY.MMM.DD", "yyyy.MMM.d");

        userToJavaTimeFormatMap.put("00:00:00 12H", "h:m:s a");
        userToJavaTimeFormatMap.put("00:00:00 24H", "H:m:s");
        userToJavaTimeFormatMap.put("00-00-00 12H", "h-m-s a");
        userToJavaTimeFormatMap.put("00-00-00 24H", "H-m-s");
        userToJavaTimeFormatMap.put("00 00 00 12H", "h m s a");
        userToJavaTimeFormatMap.put("00 00 00 24H", "H m s");

        userToJavaTimeFormatMap.put("00:00 12H", "h:m a");
        userToJavaTimeFormatMap.put("00:00 24H", "H:m");
        userToJavaTimeFormatMap.put("00-00 12H", "h-m a");
        userToJavaTimeFormatMap.put("00-00 24H", "H-m");
        userToJavaTimeFormatMap.put("00 00 12H", "h m a");
        userToJavaTimeFormatMap.put("00 00 24H", "H m");
    }

    // Set up static mappings for user exposed time zone format to Java 8 format.
    static {
        //                         User Time Zone             Java Time Zone             UTC Offset when in Standard
        //                                                                               (Non-Daylight Savings) Time
        userToJavaTimeZoneMap.put("UTC",                     "UTC");                     // UTC
        userToJavaTimeZoneMap.put("Africa/Accra",            "Africa/Accra");            // UTC
        userToJavaTimeZoneMap.put("Africa/Cairo",            "Africa/Cairo");            // UTC+2
        userToJavaTimeZoneMap.put("Africa/Lagos",            "Africa/Lagos");            // UTC+1
        userToJavaTimeZoneMap.put("America/Anchorage",       "America/Anchorage");       // UTC-9
        userToJavaTimeZoneMap.put("America/Buenos_Aires",    "America/Buenos_Aires");    // UTC-3
        userToJavaTimeZoneMap.put("America/Chicago",         "America/Chicago");         // UTC-6
        userToJavaTimeZoneMap.put("America/Halifax",         "America/Halifax");         // UTC-4
        userToJavaTimeZoneMap.put("America/Lima",            "America/Lima");            // UTC-5
        userToJavaTimeZoneMap.put("America/Los_Angeles",     "America/Los_Angeles");     // UTC-8
        userToJavaTimeZoneMap.put("America/Mexico_City",     "America/Mexico_City");     // UTC-6
        userToJavaTimeZoneMap.put("America/New_York",        "America/New_York");        // UTC-5
        userToJavaTimeZoneMap.put("America/Phoenix",         "America/Phoenix");         // UTC-7
        userToJavaTimeZoneMap.put("America/Santiago",        "America/Santiago");        // UTC-4
        userToJavaTimeZoneMap.put("America/Sao_Paulo",       "America/Sao_Paulo");       // UTC-3
        userToJavaTimeZoneMap.put("America/St_Johns",        "America/St_Johns");        // UTC-3:30
        userToJavaTimeZoneMap.put("Asia/Almaty",             "Asia/Almaty");             // UTC+6
        userToJavaTimeZoneMap.put("Asia/Baku",               "Asia/Baku");               // UTC+4
        userToJavaTimeZoneMap.put("Asia/Bangkok",            "Asia/Bangkok");            // UTC+7
        userToJavaTimeZoneMap.put("Asia/Colombo",            "Asia/Colombo");            // UTC+5:30
        userToJavaTimeZoneMap.put("Asia/Dhaka",              "Asia/Dhaka");              // UTC+6
        userToJavaTimeZoneMap.put("Asia/Dubai",              "Asia/Dubai");              // UTC+4
        userToJavaTimeZoneMap.put("Asia/Istanbul",           "Asia/Istanbul");           // UTC+3
        userToJavaTimeZoneMap.put("Asia/Jakarta",            "Asia/Jakarta");            // UTC+7
        userToJavaTimeZoneMap.put("Asia/Kabul",              "Asia/Kabul");              // UTC+4:30
        userToJavaTimeZoneMap.put("Asia/Kamchatka",          "Asia/Kamchatka");          // UTC+12
        userToJavaTimeZoneMap.put("Asia/Karachi",            "Asia/Karachi");            // UTC+5
        userToJavaTimeZoneMap.put("Asia/Kathmandu",          "Asia/Kathmandu");          // UTC+5:45
        userToJavaTimeZoneMap.put("Asia/Kolkata",            "Asia/Kolkata");            // UTC+5:30
        userToJavaTimeZoneMap.put("Asia/Sakhalin",           "Asia/Sakhalin");           // UTC+11
        userToJavaTimeZoneMap.put("Asia/Seoul",              "Asia/Seoul");              // UTC+9
        userToJavaTimeZoneMap.put("Asia/Shanghai",           "Asia/Shanghai");           // UTC+8
        userToJavaTimeZoneMap.put("Asia/Tashkent",           "Asia/Tashkent");           // UTC+5
        userToJavaTimeZoneMap.put("Asia/Tehran",             "Asia/Tehran");             // UTC+3:30
        userToJavaTimeZoneMap.put("Asia/Tokyo",              "Asia/Tokyo");              // UTC+9
        userToJavaTimeZoneMap.put("Asia/Yangon",             "Asia/Yangon");             // UTC+6:30
        userToJavaTimeZoneMap.put("Atlantic/Azores",         "Atlantic/Azores");         // UTC-1
        userToJavaTimeZoneMap.put("Atlantic/Cape_Verde",     "Atlantic/Cape_Verde");     // UTC-1
        userToJavaTimeZoneMap.put("Atlantic/South_Georgia",  "Atlantic/South_Georgia");  // UTC-2
        userToJavaTimeZoneMap.put("Australia/Adelaide",      "Australia/Adelaide");      // UTC+9:30
        userToJavaTimeZoneMap.put("Australia/Eucla",         "Australia/Eucla");         // UTC+8:45
        userToJavaTimeZoneMap.put("Australia/Lord_Howe",     "Australia/Lord_Howe");     // UTC+10:30
        userToJavaTimeZoneMap.put("Australia/Perth",         "Australia/Perth");         // UTC+8
        userToJavaTimeZoneMap.put("Australia/Sydney",        "Australia/Sydney");        // UTC+10
        userToJavaTimeZoneMap.put("Brazil/DeNoronha",        "Brazil/DeNoronha");        // UTC-2
        userToJavaTimeZoneMap.put("Europe/Berlin",           "Europe/Berlin");           // UTC+1
        userToJavaTimeZoneMap.put("Europe/Kiev",             "Europe/Kiev");             // UTC+2
        userToJavaTimeZoneMap.put("Europe/London",           "Europe/London");           // UTC
        userToJavaTimeZoneMap.put("Europe/Moscow",           "Europe/Moscow");           // UTC+3
        userToJavaTimeZoneMap.put("Indian/Cocos",            "Indian/Cocos");            // UTC+6:30
        userToJavaTimeZoneMap.put("Military/Yankee",         "Etc/GMT+12");              // UTC-12
        userToJavaTimeZoneMap.put("Pacific/Apia",            "Pacific/Apia");            // UTC+13
        userToJavaTimeZoneMap.put("Pacific/Auckland",        "Pacific/Auckland");        // UTC+12
        userToJavaTimeZoneMap.put("Pacific/Guam",            "Pacific/Guam");            // UTC+10
        userToJavaTimeZoneMap.put("Pacific/Noumea",          "Pacific/Noumea");          // UTC+11
        userToJavaTimeZoneMap.put("Pacific/Chatham",         "Pacific/Chatham");         // UTC+12:45
        userToJavaTimeZoneMap.put("Pacific/Honolulu",        "Pacific/Honolulu");        // UTC-10
        userToJavaTimeZoneMap.put("Pacific/Kiritimati",      "Pacific/Kiritimati");      // UTC+14
        userToJavaTimeZoneMap.put("Pacific/Marquesas",       "Pacific/Marquesas");       // UTC-9:30
        userToJavaTimeZoneMap.put("Pacific/Pago_Pago",       "Pacific/Pago_Pago");       // UTC-11
        userToJavaTimeZoneMap.put("Pacific/Tongatapu",       "Pacific/Tongatapu");       // UTC+13
    }

    // Set up format data structures for autodetection of formats during user data import workflow.
    static {
        for (Map.Entry<String, String> userJavaDateFormat : userToJavaDateFormatMap.entrySet()) {
            javaToUserDateFormatMap.put(userJavaDateFormat.getValue(), userJavaDateFormat.getKey());
        }

        for (Map.Entry<String, String> userJavaTimeFormat : userToJavaTimeFormatMap.entrySet()) {
            javaToUserTimeFormatMap.put(userJavaTimeFormat.getValue(), userJavaTimeFormat.getKey());
        }

        // Construct all supported java date + time formats (included date only formats).
        for (String dateFormat : userToJavaDateFormatMap.values()) {
            for (String timeFormat : userToJavaTimeFormatMap.values()) {
                SUPPORTED_JAVA_DATE_TIME_FORMATS.add(String.format("%s %s", dateFormat, timeFormat));
            }
        }
        SUPPORTED_JAVA_DATE_TIME_FORMATS.addAll(userToJavaDateFormatMap.values());
        for (String format : TimeStampConvertUtils.SUPPORTED_JAVA_DATE_TIME_FORMATS) {
            SUPPORTED_DATE_TIME_FORMATTERS.add(java.time.format.DateTimeFormatter.ofPattern(format.trim()));
        }

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


    // TODO: Add additional "getters and setters" as needed.

    // Get lists of user supported date formats, time formats, and time zones.
    public static List<String> getSupportedUserDateFormats() {
        return new ArrayList<>(userToJavaDateFormatMap.keySet());
    }

    public static List<String> getSupportedUserTimeFormats() {
        return new ArrayList<>(userToJavaTimeFormatMap.keySet());
    }

    public static List<String> getSupportedUserTimeZones() {
        return new ArrayList<>(userToJavaTimeZoneMap.keySet());
    }

    // Get list of Java supported time zones.
    public static List<String> getSupportedJavaTimeZones() {
        return new ArrayList<>(userToJavaTimeZoneMap.values());
    }

    public static String mapJavaToUserDateFormat(String javaDateFormat) {
        return javaToUserDateFormatMap.get(javaDateFormat);
    }

    public static String mapJavaToUserTimeFormat(String javaTimeFormat) {
        return javaToUserTimeFormatMap.get(javaTimeFormat);
    }

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

    // Enhanced date/time conversion which leverages user defined date format, time format, and time zone
    // strings to provide broader date/time string to timestamp conversion.  If no date format is provided
    // the function reverts to using the original convertToLong() function above.  If no time format is
    // provided, it processes the provided date/time string as if there is only a date component.  If no
    // time zone is provided, it assume UTC.  The function throws exceptions if incorrect date format,
    // time format, or time zone strings are provided.
    public static long convertToLong(String dateTime, String userDateFormatStr, String userTimeFormatStr,
                                     String userTimeZoneStr) {
        log.debug("Date is: " + dateTime + "  Date Format is: " + userDateFormatStr
                + "  Time Format is: " + userTimeFormatStr + "  Time Zone is: " + userTimeZoneStr);

        // Remove excessive whitespace from the date/time value.  First trim the beginning and end.
        dateTime = dateTime.trim();
        // Now turn any whitespace larger than one space into exactly one space.
        // NOTE: This won't work if date formats have whitespace in them.
        dateTime = dateTime.replaceFirst("(\\s\\s+)", " ");

        // Heuristic for https://solutions.lattice-engines.com/browse/DP-9768.
        // Assume any contiguous set of 3 or more letters is a month in long format and convert it to Title Case
        // (first letter capitalized) to allow Java 8 DateTimeFormatter to work properly.
        Matcher monthMatcher = MONTH.matcher(dateTime);
        if (monthMatcher.find() && monthMatcher.groupCount() == 2) {
            log.debug("Reformatting text form of month to proper case for Java DateTimeFormatter.");
            log.debug("Original dateTime is: " + dateTime);
            //log.debug("Group 1 is: " + matcher.group(1));
            //log.debug("Group 2 is: " + matcher.group(2));
            dateTime = dateTime.replaceFirst(monthMatcher.group(1) + monthMatcher.group(2),
                    monthMatcher.group(1).toUpperCase() + monthMatcher.group(2).toLowerCase());
            log.debug("Formatted month dateTime is: " + dateTime);
        }

        try {
            // Check if a date format string is provided which allows the usage of LocalDateTime from Java 8.
            if (StringUtils.isNotEmpty(userDateFormatStr)) {
                userDateFormatStr = userDateFormatStr.trim();
                // Check if the date format string is in the set of accepted formats.
                if (userToJavaDateFormatMap.containsKey(userDateFormatStr)) {
                    LocalDateTime localDateTime = null;
                    boolean foundValidTimeFormat = false;
                    log.debug("Found user defined date format: " + userDateFormatStr);
                    String javaDateFormatStr = userToJavaDateFormatMap.get(userDateFormatStr);
                    String javaFormatUsed = javaDateFormatStr;

                    // If the time format string is not empty, make sure it matches an accepted format.
                    if (StringUtils.isNotEmpty(userTimeFormatStr)) {
                        userTimeFormatStr = userTimeFormatStr.trim();
                        if (userToJavaTimeFormatMap.containsKey(userTimeFormatStr)) {
                            log.debug("Found user defined time format: " + userTimeFormatStr);
                            String javaTimeFormatStr = userToJavaTimeFormatMap.get(userTimeFormatStr);

                            log.debug("Java date/time format string is: " + javaDateFormatStr + " "
                                    + javaTimeFormatStr);

                            Matcher dateTimeMatcher = TZ_DATE_TIME.matcher(dateTime);
                            if (dateTimeMatcher.find()) {
                                log.debug("Found Date/Time value with ISO 8601 format (T & Z): " + dateTime);
                                //log.debug("Regular expression matches with " + dateTimeMatcher.groupCount()
                                //        + " group(s)");
                                //for (int i = 0; i <= dateTimeMatcher.groupCount(); i++) {
                                //    log.debug("  Group " + i + ": " + dateTimeMatcher.group(i));
                                //}
                                dateTime = dateTimeMatcher.group(1) + " " + dateTimeMatcher.group(4);
                                log.debug("Stripping T & Z to end up with: " + dateTime);
                            }
                            //else {
                            //    log.debug("No regular expression match");
                            //}

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
                                // TODO(jwinter): Consider whether we should strip out invalid time components of a
                                //     date/time even when a time format is provided but doesn't match the actual time
                                //     value.  The risk is silence errors where the user used the wrong time format and
                                //     it is ignored by the code here but the user never even realizes.
                                /*
                                // When parsing date and time doesn't work, try just parsing out a date from the value
                                // as a backup plan.  Strip off extra characters from the date/time which represent the
                                // unparsable time component of the date/time.  Note that this will not work if there is
                                // white space inside the date format, but this is just a heuristic backup plan.
                                log.warn("Could not parse date/time from: " + dateTime
                                        + ".  Trying to strip time component and parse only date.");
                                String dateWithTimeStripped = dateTime.replaceFirst("(\\s+.+)", "");
                                log.warn("Date value after stripping trailing characters: " + dateWithTimeStripped);
                                try {
                                    localDateTime = LocalDate.parse(dateWithTimeStripped,
                                            java.time.format.DateTimeFormatter.ofPattern(javaDateFormatStr))
                                            .atStartOfDay();
                                */

                                // When parsing date and time doesn't work, try just parsing out a date from the value
                                // as a backup plan.
                                try {
                                    log.warn("Could not parse date/time from: " + dateTime
                                            + ".  Trying to parse only date");
                                    localDateTime = LocalDate.parse(dateTime,
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
                        log.debug("Time format string was empty.  Using on date format.");
                        log.debug("Java date only format string is: " + javaDateFormatStr);
                        // Parse the date value provided using DateTimeFormatter with only a date component.
                        try {
                            localDateTime = LocalDate.parse(dateTime,
                                    java.time.format.DateTimeFormatter.ofPattern(javaDateFormatStr)).atStartOfDay();
                        } catch (DateTimeParseException e) {
                            // When parsing a date doesn't work, try cutting off extra characters from the date/time
                            // string which might represent a time.  Note that this will not work if there is white
                            // space inside the date format, but this is just a heuristic backup plan.
                            log.warn("Could not parse date from: " + dateTime + ".  Trying to strip time component");
                            String dateWithTimeStripped = dateTime.replaceFirst("(\\s+.+)", "");
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
                    log.debug("LocalDateTime is: " + localDateTime.format(
                            java.time.format.DateTimeFormatter.ofPattern(javaFormatUsed)));

                    // Now process the user provided time zone string, if provided.
                    ZoneId zoneId;
                    if (StringUtils.isNotEmpty(userTimeZoneStr)) {
                        userTimeZoneStr = userTimeZoneStr.trim();
                        if (userToJavaTimeZoneMap.containsKey(userTimeZoneStr)) {
                            log.debug("Found user defined time zone: " + userTimeZoneStr);
                            String javaTimeZoneStr = userToJavaTimeZoneMap.get(userTimeZoneStr);
                            log.debug("Java time zone string is: " + javaTimeZoneStr);
                            zoneId = TimeZone.getTimeZone(javaTimeZoneStr).toZoneId();
                        } else {
                            log.error("User provided time zone is not supported: " + userTimeZoneStr);
                            throw new IllegalArgumentException("User provided time zone is not supported: "
                                    + userTimeZoneStr);
                        }
                        log.debug("Using user provided time zone: " + zoneId.getId());
                    } else {
                        log.debug("User provided time zone string is empty, using UTC");
                        zoneId = ZoneId.of("UTC");
                    }

                    // Convert the LocalDateTime to a millisecond timestamp according the the provided (or UTC default)
                    // timezone.
                    long timestamp = localDateTime.atZone(zoneId).toInstant().toEpochMilli();
                    log.debug("Millisecond timestamp is: " + timestamp);

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
     * using java 8, DateTimeFormatter wasn't working with 210 formats added,
     * used SUPPORTED_DATE_TIME_FORMATTERS which is a list of formatters instead
     */
    public static LocalDate parseDateTime(String value) {
        LocalDate dateTime = null;
        for (java.time.format.DateTimeFormatter formatter : SUPPORTED_DATE_TIME_FORMATTERS) {
            try {
                dateTime = LocalDate.parse(value, formatter);
            } catch (Exception e) {
            }
            if (dateTime != null) {
                break;
            }
        }
        return dateTime;
    }
}
