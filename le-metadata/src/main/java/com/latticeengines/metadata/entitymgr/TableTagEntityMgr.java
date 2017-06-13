package com.latticeengines.metadata.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableTag;

public interface TableTagEntityMgr extends BaseEntityMgr<TableTag> {

    List<Table> getTablesForTag(String tagName);

    List<TableTag> getTableTagsForName(String tagName);

    void tagTable(Table table, String tagName);

    void untagTable(String tableName, String tagName);
}
