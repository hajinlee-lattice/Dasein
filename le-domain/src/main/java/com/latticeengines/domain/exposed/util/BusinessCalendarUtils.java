package com.latticeengines.domain.exposed.util;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.Calendar;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;

public final class BusinessCalendarUtils {

    private static String STANDARD_MODE_NOTE = "Business calendar mode is STANDARD.";

    public static String validate(BusinessCalendar calendar) {
        if (!Arrays.asList(1, 2, 3).contains(calendar.getLongerMonth())) {
            String msg = "Longer Month can only be 1, 2 or 3.";
            Exception exception = new IllegalArgumentException(msg);
            throw new LedpException(LedpCode.LEDP_40015, msg, exception);
        }

        Integer evaluationYear = calendar.getEvaluationYear();
        if (evaluationYear == null || evaluationYear < 1990) {
            evaluationYear = getCurrentYear();
        }

        if (calendar.getMode() == null) {
            String msg = "Business calendar mode cannot be empty";
            Exception exception = new UnsupportedOperationException(msg);
            throw new LedpException(LedpCode.LEDP_40060, msg, exception);
        }

        switch (calendar.getMode()) {
            case STARTING_DAY:
                return validateStartingDay(calendar.getStartingDay(), evaluationYear);
            case STARTING_DATE:
                return validateStartingDate(calendar.getStartingDate(), evaluationYear);
            case STANDARD:
                calendar.setStartingDate(null);
                calendar.setStartingDay(null);
                calendar.setLongerMonth(null);
                return STANDARD_MODE_NOTE;
            default:
                String msg = "Unknown business calendar mode " + calendar.getMode();
                Exception exception = new UnsupportedOperationException(msg);
                throw new LedpException(LedpCode.LEDP_40015, msg, exception);
        }
    }

    public static Pair<LocalDate, LocalDate> parseDateRangeFromStartDay(String startingDay, int evaluationYear) {
        LocalDate startDate = parseLocalDateFromStartingDay(startingDay, evaluationYear);
        LocalDate endDate = parseLocalDateFromStartingDay(startingDay, evaluationYear + 1)
                .minusDays(1);
        return Pair.of(startDate, endDate);
    }

    public static LocalDate parseLocalDateFromStartingDay(String startingDay, int evaluationYear) {
        if (StringUtils.isBlank(startingDay)) {
            String msg = "Cannot configure business calendar with empty starting day.";
            IllegalArgumentException exception = new IllegalArgumentException(msg);
            throw new LedpException(LedpCode.LEDP_40015, msg, exception);
        }
        String[] tokens = startingDay.split("-");
        if (tokens.length != 3) {
            throw new LedpException(LedpCode.LEDP_40015, new String[] { startingDay });
        }

        int idx = Arrays.asList("1st", "2nd", "3rd", "4th").indexOf(tokens[0].toLowerCase());
        if (idx < 0) {
            String msg = "Only \"1st\", \"2nd\", \"3rd\", \"4th\" are allowed as the first token of a starting day.";
            IllegalArgumentException exception = new IllegalArgumentException(msg);
            throw new LedpException(LedpCode.LEDP_40015, msg, exception);
        }

        int dayOfWeek = getDayOfWeek(tokens[1]);
        if (dayOfWeek == -1) {
            throw new LedpException(LedpCode.LEDP_40015, new String[] { startingDay });
        }

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MMM-dd");
        String monthStr = tokens[2];
        monthStr = monthStr.substring(0, 1).toUpperCase() + monthStr.substring(1).toLowerCase();
        String fullDate = String.format("%04d-%s-01", evaluationYear, monthStr);
        int month;
        try {
            LocalDate localDate = LocalDate.parse(fullDate, formatter);
            month = localDate.getMonthValue();
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_40015, e, new String[] { startingDay });
        }

        try {
            return parseDate(month, idx, dayOfWeek, evaluationYear);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_40015, e, new String[] { startingDay });
        }
    }

    public static Pair<LocalDate, LocalDate> parseDateRangeFromStartDate(String startingDate, int evaluationYear) {
        LocalDate startDate = parseLocalDateFromStartingDate(startingDate, evaluationYear);
        LocalDate endDate = parseLocalDateFromStartingDate(startingDate, evaluationYear + 1)
                .minusDays(1);
        return Pair.of(startDate, endDate);
    }

    public static LocalDate parseLocalDateFromStartingDate(String startingDate, int evaluationYear) {
        if (StringUtils.isBlank(startingDate)) {
            throw new LedpException(LedpCode.LEDP_40015, new String[] { startingDate });
        }
        if ("FEB-29".equals(startingDate.toUpperCase())) {
            String msg = "Should not use February 29th to configure your business calendar.";
            IllegalArgumentException exception = new IllegalArgumentException(msg);
            throw new LedpException(LedpCode.LEDP_40015, msg, exception);
        }

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MMM-dd");
        String formattedDate = startingDate.substring(0, 1).toUpperCase()
                + startingDate.substring(1).toLowerCase();
        String fullDate = String.format("%04d-%s", evaluationYear, formattedDate);
        try {
            return LocalDate.parse(fullDate, formatter);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_40015, e, new String[] { formattedDate });
        }
    }

    private static String validateStartingDay(String startingDay, int evaluationYear) {
        Pair<LocalDate, LocalDate> dateRange = parseDateRangeFromStartDay(startingDay,
                evaluationYear);
        return getStartingDayNote(dateRange.getLeft(), dateRange.getRight(), evaluationYear);
    }

    private static String getStartingDayNote(LocalDate startDate, LocalDate endDate,
            int evaluationYear) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd MMMM yyyy");
        int days = (int) Duration.between(startDate.atStartOfDay(), endDate.atStartOfDay()).toDays()
                + 1;
        int weeks = days / 7;
        return String.format("The Fiscal Year **%d** has **%d** weeks, **%s** to **%s**.",
                evaluationYear, weeks, startDate.format(formatter), endDate.format(formatter));
    }

    private static LocalDate parseDate(int month, int idx, int dayOfWeek, int evaluationYear) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.DAY_OF_WEEK, dayOfWeek);
        calendar.set(Calendar.DAY_OF_WEEK_IN_MONTH, idx + 1);
        calendar.set(Calendar.MONTH, month - 1);
        calendar.set(Calendar.YEAR, evaluationYear);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        return LocalDate.parse(format.format(calendar.getTime()));
    }

    private static int getDayOfWeek(String dayStr) {
        int day = -1;
        if ("SUN".equalsIgnoreCase(dayStr)) {
            day = Calendar.SUNDAY;
        } else if ("MON".equalsIgnoreCase(dayStr)) {
            day = Calendar.MONDAY;
        } else if ("TUE".equalsIgnoreCase(dayStr)) {
            day = Calendar.TUESDAY;
        } else if ("WED".equalsIgnoreCase(dayStr)) {
            day = Calendar.WEDNESDAY;
        } else if ("THU".equalsIgnoreCase(dayStr)) {
            day = Calendar.THURSDAY;
        } else if ("FRI".equalsIgnoreCase(dayStr)) {
            day = Calendar.FRIDAY;
        } else if ("SAT".equalsIgnoreCase(dayStr)) {
            day = Calendar.SATURDAY;
        }
        return day;
    }

    private static String validateStartingDate(String startingDate, int evaluationYear) {
        parseDateRangeFromStartDate(startingDate, evaluationYear);
        return getStartingDateNote(evaluationYear);
    }

    private static String getStartingDateNote(int evaluationYear) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate localDate = LocalDate.parse(String.format("%04d-02-01", evaluationYear),
                formatter);
        int daysInLastWeek = localDate.isLeapYear() ? 9 : 8;
        return String.format("The last week of Fiscal Year **%4d** will have **%d** days.",
                evaluationYear, daysInLastWeek);
    }

    private static int getCurrentYear() {
        return LocalDate.now().get(ChronoField.YEAR);
    }

}
