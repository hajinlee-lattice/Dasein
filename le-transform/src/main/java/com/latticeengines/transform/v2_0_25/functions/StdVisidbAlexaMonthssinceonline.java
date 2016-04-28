package com.latticeengines.transform.v2_0_25.functions;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Period;

import com.latticeengines.transform.exposed.RealTimeTransform;

public class StdVisidbAlexaMonthssinceonline implements RealTimeTransform {

    public StdVisidbAlexaMonthssinceonline(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments,
            Map<String, Object> record) {
        String column = (String) arguments.get("column");
        String s = column == null ? null : String.valueOf(record.get(column));

        if(s.equals("null"))
            return null;

        return calculateStdVisidbAlexaMonthssinceonline(s);
    }

    public static Integer calculateStdVisidbAlexaMonthssinceonline(String date) {
        if (StringUtils.isEmpty(date) || "null".equals(date))
            return null;

        Date dt = null;

        if (Pattern.matches("\\d+", date)) {
            try {
                Long dateAsLong = new Long(date);
                dt = new Date(dateAsLong);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        } else {
            // Needs to match datetime.datetime.strptime(date, '%m/%d/%Y
            // %I:%M:%S %p')
            SimpleDateFormat format = new SimpleDateFormat(
                    "MM/dd/yy HH:mm:ss a");
            try {
                dt = format.parse(date);
            } catch (ParseException e) {
                e.printStackTrace();
                return null;
            }
        }

        Period p = new Period(new DateTime(dt.getTime()), DateTime.now());

        return p.getYears() * 12 + p.getMonths();
    }
}
