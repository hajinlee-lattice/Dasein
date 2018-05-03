package com.latticeengines.proxy.cdl;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

@ContextConfiguration(locations = { "classpath:test-proxy-context.xml" })
public class BaseAttrConfigProxyImplTestNG extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(BaseAttrConfigProxyImplTestNG.class);

    private static String customerSpace = "tenant";

    private static final String[] propertyNames = { ColumnSelection.Predefined.Segment.getName(),
            ColumnSelection.Predefined.Enrichment.getName(), ColumnSelection.Predefined.TalkingPoint.getName(),
            ColumnSelection.Predefined.CompanyProfile.getName() };
    private static final String[] categoryNames = { Category.INTENT.getName(), Category.TECHNOLOGY_PROFILE.getName() };

    @Test(groups = "unit")
    public void testContructUrlForGetAttrConfigOverview() {
        CDLAttrConfigProxyImpl cdlAttrConfigProxy = new CDLAttrConfigProxyImpl();
        String url = cdlAttrConfigProxy.contructUrlForGetAttrConfigOverview(customerSpace, Arrays.asList(categoryNames),
                Arrays.asList(propertyNames), true);
        log.info("url is " + url);

        url = cdlAttrConfigProxy.contructUrlForGetAttrConfigOverview(customerSpace, null, Arrays.asList(propertyNames),
                true);
        log.info("url is " + url);

    }

}
