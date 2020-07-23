package com.latticeengines.spark.exposed.job.dcp;

import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.Classification;
import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.ConfidenceCode;
import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.MatchedDuns;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.dcp.PrepareDataReportConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class PrepareDataReportJobTestNG extends SparkJobFunctionalTestNGBase {
    private static final List<Pair<String, Class<?>>> FIELDS = Arrays.asList(
            Pair.of("CustomerID", String.class),
            Pair.of(InterfaceName.PhoneNumber.name(), String.class),
            Pair.of(InterfaceName.State.name(), String.class),
            Pair.of(InterfaceName.Country.name(), String.class),
            Pair.of(InterfaceName.Website.name(), String.class),
            Pair.of(MatchedDuns, String.class),
            Pair.of(Classification, String.class),
            Pair.of(ConfidenceCode, Integer.class)
    );

    @Test(groups = "functional")
    public void testDataReport() {
        String input = uploadData();
        PrepareDataReportConfig config = new PrepareDataReportConfig();
        config.setNumTargets(1);
        config.setMatchedDunsAttr(MatchedDuns);
        config.setClassificationAttr(Classification);
        SparkJobResult result = runSparkJob(PrepareDataReportJob.class, config, Collections.singletonList(input),
                getWorkspace());
        verifyResult(result);
    }

    @Override
    public Boolean verifySingleTarget(HdfsDataUnit tgt) {
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            System.out.println(record);
        });
        return true;
    }

    private String uploadData() {
        String accepted = DnBMatchCandidate.Classification.Accepted.name();
        String rejected = DnBMatchCandidate.Classification.Rejected.name();
        Object[][] data = new Object[][] {
                {"1", "234-567", "California", "United States", "3i.com", "123456", accepted, 1},
                {"2", "121-567", "New York", "United States", "3k.com", "234567", accepted, 2},
                {"3", "123-567", "Illinois", "United States", "abbott.com", "345678", accepted, 3},
                {"4", "234-888", "Guangdong", "China", "qq.com", "456789", accepted, 4},
                {"5", "222-333", "Paris", "France", "accor.com", "456789", accepted, 5},
                {"6", "666-999", "UC", "United States", "3i.com", "456789", accepted, 6},
                {"7", "888-056", " ", null, "adecco.com", "123456", accepted, 7},
                {"8", "777-056", "Zhejiang", "China", "alibaba.com", null, rejected, 0}
        };
        return uploadHdfsDataUnit(data, FIELDS);
    }
}
