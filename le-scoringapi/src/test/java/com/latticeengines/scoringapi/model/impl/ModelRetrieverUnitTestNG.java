package com.latticeengines.scoringapi.model.impl;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;

public class ModelRetrieverUnitTestNG {

    @Test(groups = "unit")
    public void testRemoveDroppedDataScienceFieldEventTableTransforms() throws IOException {
        URL eventTableDataCompositionUrl = ClassLoader
                .getSystemResource("com/latticeengines/scoringapi/model/eventtable-datacomposition.json");
        String eventTableDataCompositionContents = Files.toString(new File(eventTableDataCompositionUrl.getFile()),
                Charset.defaultCharset());
        DataComposition eventTableDataComposition = JsonUtils.deserialize(eventTableDataCompositionContents,
                DataComposition.class);

        URL dataScienceDataCompositionUrl = ClassLoader
                .getSystemResource("com/latticeengines/scoringapi/model/datascience-datacomposition.json");
        String dataScienceDataCompositionContents = Files.toString(new File(dataScienceDataCompositionUrl.getFile()),
                Charset.defaultCharset());
        DataComposition dataScienceDataComposition = JsonUtils.deserialize(dataScienceDataCompositionContents,
                DataComposition.class);

        int original = eventTableDataComposition.transforms.size();
        ModelRetrieverImpl modelRetriever = new ModelRetrieverImpl();
        modelRetriever.removeDroppedDataScienceFieldEventTableTransforms(eventTableDataComposition,
                dataScienceDataComposition);

        Assert.assertEquals(original - eventTableDataComposition.transforms.size(), 2);
    }
}
