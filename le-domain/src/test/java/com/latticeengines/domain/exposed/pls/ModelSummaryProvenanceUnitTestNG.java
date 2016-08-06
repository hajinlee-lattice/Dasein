package com.latticeengines.domain.exposed.pls;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ModelSummaryProvenanceUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() {
        ModelSummaryProvenance modelSummaryProvenance = new ModelSummaryProvenance();
        modelSummaryProvenance.setProvenanceProperty(ProvenancePropertyName.ExcludePropdataColumns, false);
        modelSummaryProvenance.setProvenanceProperty(ProvenancePropertyName.ExcludePublicDomains, true);
        modelSummaryProvenance.setProvenanceProperty(ProvenancePropertyName.IsOneLeadPerDomain, false);
        String ser = modelSummaryProvenance.getProvenancePropertyString();
        System.out.println(String.format("model provenance is: %s", ser));
        Assert.assertTrue(ser.contains("Exclude_Propdata_Columns=false"));
        Assert.assertTrue(ser.contains("Exclude_Public_Domains=true"));
        Assert.assertTrue(ser.contains("Is_One_Lead_Per_Domain=false"));
    }
}
