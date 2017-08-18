package com.latticeengines.datacloud.yarn.runtime;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.service.PublicDomainService;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;

public class DedupeHelperImplTestNG {

    @Test(groups = "unit", dataProvider = "dataProvider")
    public void appendDedupeValues(boolean isPublicDomain, boolean hasFeatures, String latticeAccountId, String Dduns,
            String duns, String name, String domain, String country, String dedupeId, Integer isRemoved) {
        DedupeHelperImpl dedupeHelper = new DedupeHelperImpl();

        // matched
        List<Object> allValues = new ArrayList<>();
        ProcessorContext processorContext = new ProcessorContext();
        MatchInput matchInput = new MatchInput();
        matchInput.setPublicDomainAsNormalDomain(false);
        processorContext.setOriginalInput(matchInput);

        OutputRecord outputRecord = new OutputRecord();
        outputRecord.setMatchedLatticeAccountId(latticeAccountId);
        outputRecord.setMatchedDduns(Dduns);
        outputRecord.setMatchedDuns(duns);
        outputRecord.setPreMatchDomain(domain);
        NameLocation nameLocation = new NameLocation();
        nameLocation.setName(name);
        nameLocation.setCountry(country);
        outputRecord.setPreMatchNameLocation(nameLocation);
        if (hasFeatures) {
            outputRecord.setOutput(Arrays.asList("FirstName"));
            outputRecord.setNumFeatureValue(1);
        } else {
            outputRecord.setOutput(new ArrayList<Object>());
        }

        PublicDomainService publicDomainService = mock(PublicDomainService.class);
        when(publicDomainService.isPublicDomain(domain)).thenReturn(isPublicDomain);
        dedupeHelper.publicDomainService = publicDomainService;
        dedupeHelper.appendDedupeValues(processorContext, allValues, outputRecord);

        Assert.assertEquals(allValues.size(), 3);
        Assert.assertEquals(allValues.get(0), latticeAccountId);
        Assert.assertEquals(allValues.get(1), dedupeId);
        Assert.assertEquals(allValues.get(2), isRemoved);
    }

    @DataProvider(name = "unit")
    public Object[][] dataProvider() {
        return new Object[][] { //
                new Object[] { false, true, "111", "112", "113", null, "lattice.com", "USA", "112", 0 }, //
                new Object[] { false, true, "111", null, "113", null, "lattice.com", "USA", "113", 0 }, //
                new Object[] { false, true, "111", null, null, null, "lattice.com", "USA", "lattice.com", 0 }, //

                new Object[] { true, false, null, null, null, null, "lattice.com", null, null, 1 }, //

                new Object[] { false, true, null, null, null, "lattice", null, "USA", "Nw79Her_B75t4qmy5lXDGQ", 0 }, //
                new Object[] { false, true, null, null, null, "lattice", null, null, "Nw79Her_B75t4qmy5lXDGQ", 0 }, //
                new Object[] { false, true, null, null, null, null, "lattice.com", "USA", "6Lz7PZ3j8botd6YLHFYggA", 0 }, //
                new Object[] { false, true, null, null, null, null, "lattice.com", null, "O1PHfxXxmvFFBwBNaGYdXQ", 0 }, //
                new Object[] { false, true, null, null, null, "lattice", "lattice.com", "USA",
                        "6Lz7PZ3j8botd6YLHFYggA", 0 }, //
                new Object[] { false, true, null, null, null, "lattice", "lattice.com", null, "O1PHfxXxmvFFBwBNaGYdXQ",
                        0 }, //
                new Object[] { false, true, null, null, null, null, null, null, null, 1 }, //
                new Object[] { false, true, null, null, null, " none ", null, null, null, 1 }, //
                new Object[] { false, true, null, null, null, " no ", null, null, null, 1 }, //
                new Object[] { false, true, null, null, null, " [noT ", null, null, null, 1 }, //
                new Object[] { false, true, null, null, null, " delete ", null, null, null, 1 }, //
                new Object[] { false, true, null, null, null, " asd ", null, null, null, 1 }, //
                new Object[] { false, true, null, null, null, " sdf ", null, null, null, 1 }, //
                new Object[] { false, true, null, null, null, " unknown ", null, null, null, 1 }, //
                new Object[] { false, true, null, null, null, " undisclosed ", null, null, null, 1 }, //
                new Object[] { true, true, null, null, null, " null ", "lattice.com", "USA", null, 1 }, //
                new Object[] { false, true, null, null, null, " [[doNT ", null, null, null, 1 }, //
                new Object[] { false, true, null, null, null, " [[doN'T ", null, null, null, 1 }, //
                new Object[] { false, true, null, null, null, " n.a ", null, null, null, 1 }, //
                new Object[] { true, true, null, null, null, " n/a ", "lattice.com", "USA", null, 1 }, //
                new Object[] { true, true, null, null, null, " n/a ", "lattice.com", "USA", null, 1 }, //
                new Object[] { true, true, null, null, null, "abc", "lattice.com", "USA", null, 1 }, //
                new Object[] { true, true, null, null, null, " xyz ", "lattice.com", "USA", null, 1 }, //
                new Object[] { true, true, null, null, null, " noname ", "lattice.com", "USA", null, 1 }, //
                new Object[] { true, true, null, null, null, " nocompany ", "lattice.com", "USA", null, 1 }, //

                new Object[] { false, true, null, null, null, " nocompany ", "lattice.com", "USA", "6Lz7PZ3j8botd6YLHFYggA", 0 }, //

                new Object[] { true, true, null, null, null, "lattice", null, "USA", "Nw79Her_B75t4qmy5lXDGQ", 0 }, //
                new Object[] { true, true, null, null, null, "lattice", null, null, "Nw79Her_B75t4qmy5lXDGQ", 0 }, //

        };
    }
}
