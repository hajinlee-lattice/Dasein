package com.latticeengines.domain.exposed.serviceflows.cdl.steps.rematch;

import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.BaseConvertBatchStoreServiceConfiguration;

public class RematchConvertServiceConfiguration extends BaseConvertBatchStoreServiceConfiguration {

    @JsonProperty("entity")
    private BusinessEntity entity;

    @JsonProperty("need_convert_batchstore_tables")
    private HashMap<TableRoleInCollection, Table> needConvertBatchStoreTables;

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    public HashMap<TableRoleInCollection, Table> getNeedConvertBatchStoreTables() {
        return needConvertBatchStoreTables;
    }

    public void setNeedConvertBatchStoreTables(HashMap<TableRoleInCollection, Table> needConvertBatchStoreTables) {
        this.needConvertBatchStoreTables = needConvertBatchStoreTables;
    }
}
