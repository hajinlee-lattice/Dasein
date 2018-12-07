package com.latticeengines.domain.exposed.metadata;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

public class DataUnitUnitTestNG {

    @Test(groups = "unit")
    private void testSerDe() {
        HdfsDataUnit unit = new HdfsDataUnit();
        unit.setPath("/app/2");
        unit.setName("name");
        List<DataUnit> units = Collections.singletonList(unit);
        String serialized = JsonUtils.serialize(units);
        System.out.println(serialized);
    }

}
