package com.latticeengines.documentdb.entity;

import javax.persistence.Entity;
import javax.persistence.Index;
import javax.persistence.Table;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnitStore;

@Entity
@Table(name = "DataUnitStore", //
        indexes = { @Index(name = "IX_NAME", columnList = "Name") })
public class DataUnitStoreEntity extends BaseMultiTenantDocEntity<DataUnitStore> {


}
