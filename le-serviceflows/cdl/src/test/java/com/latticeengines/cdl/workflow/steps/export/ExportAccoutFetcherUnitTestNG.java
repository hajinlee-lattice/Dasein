package com.latticeengines.cdl.workflow.steps.export;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.query.DataPage;

public class ExportAccoutFetcherUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(ExportAccoutFetcherUnitTestNG.class);

    private MatchOutput matchOutput;

    private ExportAccountFetcher fetcher;

    @BeforeClass
    @Test(groups = "unit")
    public void setup() {
        matchOutput = new MatchOutput(UUID.randomUUID().toString());
        matchOutput.setOutputFields(Arrays.asList("Field1", "Field2"));
        OutputRecord outputRecord = new OutputRecord();
        outputRecord.setMatched(true);
        outputRecord.setOutput(Arrays.asList("abc", new Long(123)));
        List<OutputRecord> result = Arrays.asList(outputRecord);
        matchOutput.setResult(result);
        fetcher = new ExportAccountFetcher();
    }

    @Test(groups = "unit")
    public void test() {
        DataPage dataPage = fetcher.convertToDataPage(matchOutput);
        Assert.assertNotNull(dataPage);
        log.info(JsonUtils.serialize(dataPage));
    }

}
