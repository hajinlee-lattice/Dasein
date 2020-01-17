package com.latticeengines.transform.v2_0_25.functions;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.ApprovedUsage;
import com.latticeengines.transform.exposed.metadata.Category;
import com.latticeengines.transform.exposed.metadata.FundamentalType;
import com.latticeengines.transform.exposed.metadata.StatisticalType;
import com.latticeengines.transform.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;

public class StdVisidbAlexaMonthssinceonline implements RealTimeTransform {

    private static final Logger log = LoggerFactory.getLogger(StdVisidbAlexaMonthssinceonline.class);

    private static final long serialVersionUID = -2835201443521620065L;

    public StdVisidbAlexaMonthssinceonline() {
    }

    public StdVisidbAlexaMonthssinceonline(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        if (StringUtils.isBlank(column)) {
            return null;
        }
        return calculateStdVisidbAlexaMonthssinceonline(record.get(column));
    }

    public static Integer calculateStdVisidbAlexaMonthssinceonline(Object date) {
        if (date == null) {
            return null;
        }
        Date dt = null;
        if (date instanceof Date) { // from matching against DerivedColumnsCache
            dt = (Date) date;
        } else { // If from std_visidb_alexa_monthssinceonline.py, type is string, needs to match datetime.datetime.strptime(date, '%m/%d/%Y%I:%M:%S %p')
                 // If from file based scoring, type is string, value is timestamp in milliseconds
                 // If from matching against AccountMaster, type is long, value is timestamp in milliseconds
                 // Or unknown type from unknown source, parse will fail
            String dateStr = String.valueOf(date);
            if (StringUtils.isNotBlank(dateStr)) {
                boolean parsed = false;
                try {
                    dt = new Date(Long.valueOf(dateStr));
                    parsed = true;
                } catch (Exception ignore) {
                    // it is not a Date as Long
                }
                if (!parsed) {
                    try {
                        SimpleDateFormat format = new SimpleDateFormat("MM/dd/yy HH:mm:ss a");
                        dt = format.parse(dateStr);
                        parsed = true;
                    } catch (ParseException ignore) {
                        // it is not a SimpleDateFormat
                    }
                }
                if (!parsed) {
                    log.error("Fail to parse AlexaOnlineSince " + dateStr);
                }

            }
        }

        if (dt == null) {
            return null;
        }

        Period p = new Period(new DateTime(dt.getTime()), DateTime.now());
        return p.getYears() * 12 + p.getMonths();
    }

    @Override
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        metadata.setDataType(Integer.class.getSimpleName());
        metadata.setDisplayDiscretizationStrategy("{\"geometric\": { \"minValue\":1,\"multiplierList\":[2,2.5,2],\"minSamples\":100," //
                + "\"minFreq\":0.01,\"maxBuckets\":5,\"maxPercentile\":1}}");
        metadata.setCategory(Category.ONLINE_PRESENCE);
        metadata.setDisplayName("Months Since Online");
        metadata.setFundamentalType(FundamentalType.NUMERIC);
        metadata.setStatisticalType(StatisticalType.RATIO);
        metadata.setTags(Tag.EXTERNAL_TRANSFORM);
        metadata.setDescription("Number of months since online presence was established");
        return metadata;
    }
}
