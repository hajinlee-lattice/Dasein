package com.latticeengines.datacloud.etl.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.datacloud.ingestion.VersionCheckStrategy;

public class VersionUtils {
    
    private static Logger log = LoggerFactory.getLogger(VersionUtils.class);

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
     *            timestamp pattern in path eg. yyyy-MM-dd (currently only
     *            handles simple timestamp pattern consisted of year, month and
     *            day without timezone info)
     * @param baseCalendar:
     *            if not passed in, use current time as time base; if passed in
     *            for testing purpose, use specified time as time base
     * @return
     */
    public static List<String> getMostRecentVersionPaths(List<String> paths, Integer nPeriod,
            VersionCheckStrategy checkStrategy, String tsPattern, Calendar baseCalendar) {
        if (checkStrategy == VersionCheckStrategy.ALL) {
            return paths;
        }
        String tsRegex = tsPattern.replace("d", "\\d").replace("y", "\\d").replace("M", "\\d");
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
}
