package com.latticeengines.spark.exposed.job.cdl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ExtractListSegmentCSVConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class ExtractListSegmentCSVJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ExtractListSegmentCSVJobTestNG.class);

    private final List<String> accountAttributes = Lists.newArrayList(InterfaceName.CompanyName.name(),
            InterfaceName.Address_Street_1.name(), InterfaceName.City.name(), InterfaceName.State.name(),
            InterfaceName.Country.name(), "LDC_City", "LDC_Country", "LDC_Domain", "LDC_Industry",
            InterfaceName.LDC_Name.name(), "LDC_PostalCode", "LDC_State", "SDR_Email", "SFDC_ACCOUNT_ID",
            InterfaceName.Website.name(), InterfaceName.Industry.name());

    private final List<String> contactAttributes = Lists.newArrayList("SFDC_CONTACT_ID", InterfaceName.ContactName.name(),
            InterfaceName.Contact_Address_Street_1.name(), InterfaceName.Contact_Address_Street_2.name(),
            InterfaceName.ContactCity.name(), InterfaceName.ContactState.name(), InterfaceName.ContactCountry.name(),
            InterfaceName.Email.name(), InterfaceName.FirstName.name(), InterfaceName.LastName.name(),
            InterfaceName.PhoneNumber.name(), InterfaceName.ContactPostalCode.name(), InterfaceName.Title.name());

    @Test(groups = "functional")
    public void testGenerateRecommendationCSV() {
        uploadInputAvro();
        ExtractListSegmentCSVConfig config = buildExtractListSegmentCSVConfig();
        SparkJobResult result = runSparkJob(ExtractListSegmentCSVJob.class, config);
        verifyResult(result);
    }

    private void uploadInputAvro() {

    }

    private ExtractListSegmentCSVConfig buildExtractListSegmentCSVConfig() {
        ExtractListSegmentCSVConfig extractListSegmentCSVConfig = new ExtractListSegmentCSVConfig();
        extractListSegmentCSVConfig.setAccountAttributes(accountAttributes);
        extractListSegmentCSVConfig.setContactAttributes(contactAttributes);
        return extractListSegmentCSVConfig;
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        return true;
    }

}
