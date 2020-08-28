package com.latticeengines.datacloud.match.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.springframework.core.io.ClassPathResource;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBAPIType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchInsight;
import com.latticeengines.domain.exposed.datacloud.manage.PrimeColumn;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

public class DirectPlusUtilsUnitTestNG {

    @Test
    public void parseIdentityLocatingResult() {
        DnBMatchContext context = new DnBMatchContext();
        String response = readMockResponse("cleanseMatch");
        DirectPlusUtils.parseJsonResponse(response, context, DnBAPIType.REALTIME_ENTITY);
        Assert.assertTrue(CollectionUtils.isNotEmpty(context.getCandidates()));
        for (DnBMatchCandidate candidate: context.getCandidates()) {
            Assert.assertNotNull(candidate.getDuns());
            NameLocation nameLocation = candidate.getNameLocation();
            Assert.assertNotNull(nameLocation);
            if ("060902413".equals(candidate.getDuns())) {
                Assert.assertNotNull(nameLocation.getStreet());
                Assert.assertNotEquals(nameLocation.getStreet(), "null");
                Assert.assertNotNull(nameLocation.getState());
                Assert.assertNotEquals(nameLocation.getState(), "null");
                Assert.assertNotNull(nameLocation.getPhoneNumber());
                Assert.assertNotEquals(nameLocation.getPhoneNumber(), "null");
            }
            Assert.assertNotNull(candidate.getOperatingStatus());
            DnBMatchInsight matchInsight = candidate.getMatchInsight();
            Assert.assertNotNull(matchInsight);
            Assert.assertNotNull(matchInsight.getConfidenceCode());
            Assert.assertNotNull(matchInsight.getNameMatchScore());
        }
    }

    @Test
    public void parseDataBlock() {
        List<PrimeColumn> primeColumns = getDataBlockMetadata();

        String response = readMockResponse("compinfo");
        Map<String, Object> result = DirectPlusUtils.parseDataBlock(response, primeColumns);
        Assert.assertNotNull(result.get("TradeStyleName"));
        System.out.println(JsonUtils.pprint(result));

        Assert.assertTrue(result.get("OperatingStatusCode") instanceof Integer);

        ExecutorService tp = ThreadPoolUtils.getFixedSizeThreadPool("data-block-test", 8);
        List<Callable<Map<String, Object>>> callables = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            callables.add(() -> {
                try {
                    return DirectPlusUtils.parseDataBlock(response, primeColumns);
                } catch (Exception e) {
                    return null;
                }
            });
        }
        ThreadPoolUtils.callInParallel(tp, callables, //
                10, TimeUnit.SECONDS, 250, TimeUnit.MILLISECONDS);
        tp.shutdown();
    }

    private String readMockResponse(String name) {
        try {
            InputStream is = new ClassPathResource("direct_plus/" + name + ".json").getInputStream();
            return IOUtils.toString(is, Charset.defaultCharset());
        } catch (IOException e) {
            Assert.fail("Failed to read mock response " + name, e);
            return null;
        }
    }

    private List<PrimeColumn> getDataBlockMetadata() {
        return Arrays.asList(
                new PrimeColumn("DunsNumber", "D-U-N-S Number", "organization.duns"),
                new PrimeColumn("PrimaryBusinessName", "Primary Business Name", "organization.primaryName"),
                new PrimeColumn("TradeStyleName", "Trade Style Name", "organization.tradeStyleNames.name"),
                new PrimeColumn("PrimaryAddressStreetLine1", "Primary Address Street Line 1", "organization.primaryAddress.streetAddress.line1"),
                new PrimeColumn("PrimaryAddressStreetLine2", "Primary Address Street Line 2", "organization.primaryAddress.streetAddress.line2"),
                new PrimeColumn("PrimaryAddressLocalityName", "Primary Address Locality Name", "organization.primaryAddress.addressLocality.name"),
                new PrimeColumn("PrimaryAddressRegionName", "Primary Address Region Name", "organization.primaryAddress.addressRegion.name"),
                new PrimeColumn("PrimaryAddressPostalCode", "Primary Address Postal Code", "organization.primaryAddress.postalCode"),
                new PrimeColumn("PrimaryAddressCountryName", "Primary Address Country/Market Name", "organization.primaryAddress.addressCountry.name"),
                new PrimeColumn("TelephoneNumber", "Telephone Number", "organization.telephone.telephoneNumber"),
                new PrimeColumn("IndustryCodeUSSicV4Code", "Industry Code USSicV4 Code", "organization.primaryIndustryCode.usSicV4"),
                new PrimeColumn("BankAddressLocality", "Bank Address Locality Name", "organization.banks.address.addressLocality.name"), // test nulls and hash collisions in cache
                new PrimeColumn("OperatingStatusCode", "Operating Status Code", "organization.dunsControlStatus.operatingStatus.dnbCode", "Integer") // test integer value
        );
    }

}
