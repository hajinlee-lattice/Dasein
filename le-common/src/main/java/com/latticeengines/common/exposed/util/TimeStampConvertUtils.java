package com.latticeengines.common.exposed.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;

public class TimeStampConvertUtils {

    public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    public static long convertToLong(String date) {
        Parser parser = new Parser();
        List<DateGroup> groups = parser.parse(date);
        List<Date> dates = groups.get(0).getDates();
        return dates.get(0).getTime();
    }

    public static String convertToDate(long timeStamp){
        SimpleDateFormat sim = new SimpleDateFormat("yyyy-MM-dd");
        String date = sim.format(new Date(timeStamp));
        return date;
    }
}
