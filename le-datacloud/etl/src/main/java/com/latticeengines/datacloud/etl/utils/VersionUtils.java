package com.latticeengines.datacloud.etl.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.latticeengines.domain.exposed.datacloud.ingestion.VersionCheckStrategy;

public final class VersionUtils {

    protected VersionUtils() {
        throw new UnsupportedOperationException();
    }
    /**
     * If file/directory path contains timestamp info, return versions of most
     * recent xx months/weeks/days or all versions
     * 
     * @param paths:
     *            file/directory paths which contains timestamp info (currently
     *            only support single timestamp in path)
     * @param nPeriod:
     *            number of most recent months/weeks/days; will be ignored if
     *            VersionCheckStrategy is ALL
     * @param checkStrategy:
     *            check all versions or check based on month/week/day
     * @param tsPattern:
     *            timestamp pattern in path eg. yyyy-MM-dd
     * @param tsRegex:
     *            regex to match timestamp part; if not provided, try to convert
     *            from tsPattern (currently only support simple timestamp
     *            pattern consisted of year, month and day without timezone
     *            info)
     * @param baseCalendar:
     *            if not passed in, use current time as time base; if passed in
     *            for testing purpose, use specified time as time base
     * @return
     */
    public static List<String> getMostRecentVersionPaths(List<String> paths, Integer nPeriod,
            VersionCheckStrategy checkStrategy, String tsPattern, String tsRegex, Calendar baseCalendar) {
        if (checkStrategy == VersionCheckStrategy.ALL || StringUtils.isBlank(tsPattern)) {
            return paths;
        }
        Preconditions.checkNotNull(nPeriod);
        if (StringUtils.isBlank(tsRegex)) {
            if (!tsPattern.matches("[yMd_\\W]+")) {
                throw new IllegalArgumentException(
                        "Only support to derive timestamp regex from timestamp pattern consisted of year, month and day without timezone info");
            }
            tsRegex = tsPattern.replace("d", "\\d").replace("y", "\\d").replace("M", "\\d");
        }
        Pattern pattern = Pattern.compile(tsRegex);
        List<String> result = new ArrayList<>();
        for (String path : paths) {
            Matcher matcher = pattern.matcher(path);
            if (matcher.find()) {
                String tsStr = matcher.group();
                DateFormat df = new SimpleDateFormat(tsPattern);
                try {
                    Date ts = df.parse(tsStr);
                    Calendar calendar = baseCalendar != null ? (Calendar) baseCalendar.clone() : Calendar.getInstance();
                    switch (checkStrategy) {
                    case DAY:
                        calendar.add(Calendar.DATE, -nPeriod);
                        break;
                    case WEEK:
                        calendar.add(Calendar.DATE,
                                -nPeriod * 7 - (calendar.get(Calendar.DAY_OF_WEEK) - calendar.getFirstDayOfWeek()));
                        break;
                    case MONTH:
                        calendar.add(Calendar.MONTH, -nPeriod);
                        calendar.set(Calendar.DATE, 1);
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                String.format("Unknown file check strategy: %s", checkStrategy.toString()));
                    }
                    df = new SimpleDateFormat("yyyyMMdd");
                    Date cutOffTS = df.parse(df.format(calendar.getTime()));
                    if (ts.compareTo(cutOffTS) >= 0) {
                        result.add(path);
                    }
                } catch (ParseException e) {
                    throw new RuntimeException(String.format("Failed to parse timestamp %s", tsStr), e);
                }
            }
        }
        return result;
    }

    /**
     * Extract timestamp version part from a string
     *
     * @param inputStr:
     *            input string
     * @param originTSPattern:
     *            original timestamp pattern in input string, eg. yyyy-MM-dd
     * @param tsRegex:
     *            regex to match timestamp part in input string; if not
     *            provided, try to convert from originTSPattern (currently only
     *            support simple timestamp pattern consisted of year, month and
     *            day without timezone info)
     * @param targetTSPattern:
     *            convert to target timestamp pattern if provided, eg. yyyyMMdd;
     *            otherwise return in original timestamp pattern
     * @param timeZone:
     *            timezone of timestamp pattern
     * @return
     */
    public static String extractTSVersion(String inputStr, String originTSPattern, String tsRegex,
            String targetTSPattern, String timeZone) {
        if (StringUtils.isBlank(originTSPattern)) {
            return null;
        }
        if (StringUtils.isBlank(tsRegex)) {
            if (!originTSPattern.matches("[yMd_\\W]+")) {
                throw new IllegalArgumentException(
                        "Only support to derive timestamp regex from timestamp pattern consisted of year, month and day without timezone info");
            }
            tsRegex = originTSPattern.replace("d", "\\d").replace("y", "\\d").replace("M", "\\d");
        }
        Pattern pattern = Pattern.compile(tsRegex);
        Matcher matcher = pattern.matcher(inputStr);
        if (!matcher.find()) {
            return null;
        }
        String tsStr = matcher.group();
        if (StringUtils.isBlank(targetTSPattern)) {
            return tsStr;
        }
        DateFormat df = new SimpleDateFormat(originTSPattern);
        DateFormat targetDf = new SimpleDateFormat(targetTSPattern);
        if (StringUtils.isNotBlank(timeZone)) {
            df.setTimeZone(TimeZone.getTimeZone(timeZone));
            targetDf.setTimeZone(TimeZone.getTimeZone(timeZone));
        }
        try {
            return targetDf.format(df.parse(tsStr));
        } catch (ParseException e) {
            throw new RuntimeException(String.format("Failed to parse timestamp %s from input string", tsStr, inputStr),
                    e);
        }
    }
}
