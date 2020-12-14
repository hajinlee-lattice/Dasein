package com.latticeengines.datacloud.match.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.service.DirectPlusEnrichService;
import com.latticeengines.datacloud.match.service.PrimeMetadataService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.PrimeColumn;
import com.latticeengines.domain.exposed.datacloud.match.PrimeAccount;

public class DirectPlusEnrichServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    @Inject
    private DirectPlusEnrichService enrichService;

    @Inject
    private PrimeMetadataService primeMetadataService;

    @Test(groups = "functional")
    public void testFetchCompInfo() {
        DirectPlusEnrichRequest request = new DirectPlusEnrichRequest();
        request.setDunsNumber("651911195");
        List<PrimeColumn> reqColumns = primeMetadataService.getPrimeColumns(defaultSelection());
        request.setReqColumnsByBlockId(primeMetadataService.divideIntoBlocks(reqColumns));
        PrimeAccount result = enrichService.fetch(Collections.singleton(request)).get(0);
        Assert.assertTrue(StringUtils.isNotBlank(result.getId()));
        Assert.assertNotNull(result.getResult().get("duns_number"));
        Assert.assertNotNull(result.getResult().get("primaryname"));

        reqColumns = primeMetadataService.getPrimeColumns(expandedSelection());
        request.setReqColumnsByBlockId(primeMetadataService.divideIntoBlocks(reqColumns));
        result = enrichService.fetch(Collections.singleton(request)).get(0);
        Assert.assertTrue(StringUtils.isNotBlank(result.getId()));
        Assert.assertNotNull(result.getResult().get("latestfin_currency"));

        request.setBypassDplusCache(true);
        result = enrichService.fetch(Collections.singleton(request)).get(0);
        Assert.assertTrue(StringUtils.isNotBlank(result.getId()));
        Assert.assertNotNull(result.getResult().get("duns_number"));
        Assert.assertNotNull(result.getResult().get("primaryname"));
        Assert.assertNotNull(result.getResult().get("latestfin_currency"));
    }

    @Test(groups = "functional")
    public void testFetchNonExistDuns() {
        DirectPlusEnrichRequest request = new DirectPlusEnrichRequest();
        request.setDunsNumber("123456789");
        List<PrimeColumn> reqColumns = primeMetadataService.getPrimeColumns(defaultSelection());
        request.setReqColumnsByBlockId(primeMetadataService.divideIntoBlocks(reqColumns));
        PrimeAccount result = enrichService.fetch(Collections.singleton(request)).get(0);
        Assert.assertNotNull(result);
        Assert.assertTrue(MapUtils.isEmpty(result.getResult()));
        // SleepUtils.sleep(5000); // wait for background thread to save dynamo cache
    }

    @Test(groups = "functional")
    public void testFetchDunsUnderReview() {
        DirectPlusEnrichRequest request = new DirectPlusEnrichRequest();
        request.setDunsNumber("404090824"); // NOTE: This number is not guaranteed to work forever; works as of 2020-10-29
        List<PrimeColumn> reqColumns = primeMetadataService.getPrimeColumns(defaultSelection());
        request.setReqColumnsByBlockId(primeMetadataService.divideIntoBlocks(reqColumns));
        PrimeAccount result = enrichService.fetch(Collections.singleton(request)).get(0);
        Assert.assertNotNull(result);
        Assert.assertFalse(MapUtils.isEmpty(result.getResult()));
        Assert.assertTrue(result.getResult().containsKey(PrimeAccount.ENRICH_ERROR_CODE));
    }

    private List<String> defaultSelection() {
        return Arrays.asList( //
                "duns_number", //
                "primaryname", //
                "tradestylenames_name", //
                "primaryaddr_street_line1", //
                "primaryaddr_street_line2", //
                "primaryaddr_addrlocality_name", //
                "primaryaddr_addrregion_name", //
                "primaryaddr_postalcode", //
                "primaryaddr_country_name", //
                "telephone_telephonenumber", //
                "primaryindcode_ussicv4" //
        );
    }

    private List<String> expandedSelection() {
        return Arrays.asList( //
                "duns_number", //
                "primaryname", //
                "tradestylenames_name", //
                "latestfin_currency"
        );
    }

}
