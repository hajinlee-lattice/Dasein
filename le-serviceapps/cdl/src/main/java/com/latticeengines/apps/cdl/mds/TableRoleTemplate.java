package com.latticeengines.apps.cdl.mds;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataTemplate2;

public interface TableRoleTemplate extends DataTemplate2<TableRoleInCollection, DataCollection.Version> {

    List<ColumnMetadata> getCachedSchema(String tenantId, TableRoleInCollection tableRole, DataCollection.Version version);

}
