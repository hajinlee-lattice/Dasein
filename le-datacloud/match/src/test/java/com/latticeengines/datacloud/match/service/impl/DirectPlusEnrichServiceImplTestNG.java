package com.latticeengines.datacloud.match.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.service.DirectPlusEnrichService;
import com.latticeengines.datacloud.match.service.PrimeMetadataService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.PrimeAccount;

public class DirectPlusEnrichServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    @Inject
    private DirectPlusEnrichService enrichService;

    @Inject
    private PrimeMetadataService primeMetadataService;

    @Test(groups = "functional")
    public void testFetchCompInfo() {
        DirectPlusEnrichRequest request = new DirectPlusEnrichRequest();
        request.setDunsNumber("060902413");
        List<String> selection = defaultSelection();
        request.setReqColumns(primeMetadataService.getPrimeColumns(selection));
        request.setBlockIds(primeMetadataService.getBlocksContainingElements(selection));
        PrimeAccount result = enrichService.fetch(Collections.singleton(request)).get(0);
        Assert.assertTrue(StringUtils.isNotBlank(result.getId()));
        Assert.assertNotNull(result.getResult().get("duns_number"));
        Assert.assertNotNull(result.getResult().get("primaryname"));
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

}
