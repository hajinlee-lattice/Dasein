package com.latticeengines.eai.functionalframework;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;

public class VdbExtractAndImportUtil {

    public static Table createVdbActivity() {
        Table table = new Table();
        table.setName("Activity");
        Attribute id = new Attribute();
        id.setName("id");
        id.setDisplayName("Id");
        Attribute date = new Attribute();
        date.setName("date");
        date.setDisplayName("Date");
        table.addAttribute(id);
        table.addAttribute(date);

        PrimaryKey pk = new PrimaryKey();
        pk.setName("PK_ID");
        pk.addAttribute(id.getName());
        table.setPrimaryKey(pk);

        return table;
    }
}
