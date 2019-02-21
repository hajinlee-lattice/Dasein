package com.latticeengines.spark.exposed.job.match;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.core.spark.ParseMatchResultJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class ParseMatchResultJobRefineAndCloneTestNG extends SparkJobFunctionalTestNGBase {

    @Override
    protected String getJobName() {
        return "parseMatchResult";
    }

    @Override
    protected String getScenarioName() {
        return "RefineAndClone";
    }

    private Function<HdfsDataUnit, Boolean> verifier;

    @Test(groups = "functional")
    public void testExclude() {
        ParseMatchResultJobConfig config = new ParseMatchResultJobConfig();
        config.sourceColumns = sourceCols();
        config.excludeDataCloudAttrs = true;
        SparkJobResult result = runSparkJob(ParseMatchResultJob.class, config);
        verifier = this::verifyExcludeDCAttrs;
        verifyResult(result);
    }

    @Test(groups = "functional")
    public void testNotExclude() {
        ParseMatchResultJobConfig config = new ParseMatchResultJobConfig();
        config.sourceColumns = sourceCols();
        config.excludeDataCloudAttrs = false;
        SparkJobResult result = runSparkJob(ParseMatchResultJob.class, config);
        verifier = this::verifyNotExcludeDCAttrs;
        verifyResult(result);
    }

    private List<String> sourceCols() {
        return Arrays.asList("InternalId", "Id", "Email", "Event", "CompanyName", "City", "State", "Country",
                "CreatedDate", "FirstName", "LastName", "Title", "IsClosed", "StageName", "PhoneNumber", "Industry",
                "avro_1to300", "Activity_Count_Click_Email", "Activity_Count_Click_Link",
                "Activity_Count_Email_Bounced_Soft", "Activity_Count_Fill_Out_Form",
                "Activity_Count_Interesting_Moment_Any", "Activity_Count_Open_Email",
                "Activity_Count_Unsubscribe_Email", "Activity_Count_Visit_Webpage",
                "Activity_Count_Interesting_Moment_Email", "Activity_Count_Interesting_Moment_Event",
                "Activity_Count_Interesting_Moment_Pricing", "Activity_Count_Interesting_Moment_Webinar",
                "Activity_Count_Interesting_Moment_Multiple", "Activity_Count_Interesting_Moment_Search",
                "Activity_Count_Interesting_Moment_key_web_page", "Some_Column", "BusinessCountry", "SourceColumn");
    }

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Collections.singletonList(verifier);
    }

    private Boolean verifyExcludeDCAttrs(HdfsDataUnit target) {
        return verifyNumCols(target, 35);
    }

    private Boolean verifyNotExcludeDCAttrs(HdfsDataUnit target) {
        return verifyNumCols(target, 705);
    }

    private Boolean verifyNumCols(HdfsDataUnit target, int expected) {
        GenericRecord firstRecord = verifyAndReadTarget(target).next();
        int numCols = CollectionUtils.size(firstRecord.getSchema().getFields());
        Assert.assertEquals(numCols, expected);
        return true;
    }

}
