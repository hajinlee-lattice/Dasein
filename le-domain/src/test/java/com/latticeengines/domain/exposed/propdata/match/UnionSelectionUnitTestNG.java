package com.latticeengines.domain.exposed.propdata.match;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.domain.exposed.propdata.manage.Predefined;

public class UnionSelectionUnitTestNG {

    @Test(groups = "unit")
    public void testDeSer() {

        UnionSelection unionSelection = new UnionSelection();
        Map<Predefined, String> predefinedStringMap = new HashMap<>();
        predefinedStringMap.put(Predefined.RTS, "1.0");
        predefinedStringMap.put(Predefined.Model, "2.0");
        unionSelection.setPredefinedSelections(predefinedStringMap);
        ExternalColumn column1 = new ExternalColumn();
        column1.setExternalColumnID("column1");
        ExternalColumn column2 = new ExternalColumn();
        column1.setExternalColumnID("column2");

        ColumnSelection selection = new ColumnSelection();
        selection.createColumnSelection(Arrays.asList(column1, column2));
        unionSelection.setCustomSelection(selection);

        String serialized = JsonUtils.serialize(unionSelection);
        UnionSelection deserialized = JsonUtils.deserialize(serialized, UnionSelection.class);

        Assert.assertTrue(deserialized.getPredefinedSelections().containsKey(Predefined.RTS));
        Assert.assertTrue(deserialized.getPredefinedSelections().containsKey(Predefined.Model));
        Assert.assertEquals(deserialized.getPredefinedSelections().get(Predefined.RTS), "1.0");
        Assert.assertEquals(deserialized.getPredefinedSelections().get(Predefined.Model), "2.0");
        Assert.assertEquals(deserialized.getCustomSelection().getColumns().size(), 2);
    }

}
