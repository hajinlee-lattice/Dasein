package com.latticeengines.documentdb.entity;

import javax.persistence.Entity;
import javax.persistence.Table;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnitStore;

@Entity
@Table(name = "DataUnitStore")
public class DataUnitStoreEntity extends BaseMultiTenantDocEntity<DataUnitStore> {


}
