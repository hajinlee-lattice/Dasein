package com.latticeengines.serviceflows.workflow.modeling;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;

public class BaseModelStepUnitTestNG {

    @SuppressWarnings("rawtypes")
    @Test(groups = "unit")
    public void testGetMetadataTableFolderName() {
        BaseModelStep modelStep = new CreateModel();
        Table table = new Table();
        table.setName("Some$% Table");
        Attribute event = new Attribute();
        event.setDisplayName("dis ~-Name12");
        Assert.assertEquals(modelStep.getMetadataTableFolderName(table, event), "Some___Table-dis__-Name12-Metadata");
    }
}
