package com.latticeengines.scoringapi.exposed.model.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;

public class ModelRetrieverUnitTestNG {

    @Test(groups = "unit")
    public void testRemoveDroppedDataScienceFieldEventTableTransforms() throws IOException {
        InputStream eventTableDataCompositionIs = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("com/latticeengines/scoringapi/model/eventtable-datacomposition.json");
        String eventTableDataCompositionContents = IOUtils.toString(eventTableDataCompositionIs, Charset.defaultCharset());
        DataComposition eventTableDataComposition = JsonUtils.deserialize(eventTableDataCompositionContents,
                DataComposition.class);

        InputStream dataScienceDataCompositionIs = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("com/latticeengines/scoringapi/model/datascience-datacomposition.json");
        String dataScienceDataCompositionContents = IOUtils.toString(dataScienceDataCompositionIs,
                Charset.defaultCharset());
        DataComposition dataScienceDataComposition = JsonUtils.deserialize(dataScienceDataCompositionContents,
                DataComposition.class);

        ModelRetrieverImpl modelRetriever = new ModelRetrieverImpl();
        Map<String, FieldSchema> mergedFields = modelRetriever.mergeFields(eventTableDataComposition,
                dataScienceDataComposition);

        Assert.assertEquals(mergedFields.size(), 349);
    }
}
