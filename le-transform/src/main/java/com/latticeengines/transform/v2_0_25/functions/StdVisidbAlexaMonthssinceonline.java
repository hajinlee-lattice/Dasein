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
        if (StringUtils.isBlank(column) || record.get(column) == null) {
            return null;
        }
        return calculateStdVisidbAlexaMonthssinceonline(record.get(column));
    }

    public static Integer calculateStdVisidbAlexaMonthssinceonline(Object date) {
        Date dt = null;
        if (date instanceof Long) { // from AccountMaster
            dt = new Date((Long) date);
        } else if (date instanceof Date) { // from DerivedColumnsCache
            dt = (Date) date;
        } else { // If string from std_visidb_alexa_monthssinceonline.py, needs to match datetime.datetime.strptime(date, '%m/%d/%Y%I:%M:%S %p')
                 // Or unknown type from unknown source, parse will fail
            if (StringUtils.isNotBlank(String.valueOf(date))) {
                SimpleDateFormat format = new SimpleDateFormat("MM/dd/yy HH:mm:ss a");
                try {
                    dt = format.parse(String.valueOf(date));
                } catch (ParseException e) {
                    log.error(String.format("Fail to parse AlexaOnlneSince %s", String.valueOf(date)));
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
