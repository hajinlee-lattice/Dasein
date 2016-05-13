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
import com.latticeengines.transform.exposed.metadata.ApprovedUsage;
import com.latticeengines.transform.exposed.metadata.Category;
import com.latticeengines.transform.exposed.metadata.FundamentalType;
import com.latticeengines.transform.exposed.metadata.StatisticalType;
import com.latticeengines.transform.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;

public class StdVisidbAlexaMonthssinceonline implements RealTimeTransform {

    private static final long serialVersionUID = -2835201443521620065L;

    public StdVisidbAlexaMonthssinceonline() {
    }

    public StdVisidbAlexaMonthssinceonline(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        String s = column == null ? null : String.valueOf(record.get(column));

        if (s.equals("null"))
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
            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yy HH:mm:ss a");
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
