package com.latticeengines.documentdb.entity;

import javax.persistence.Entity;
import javax.persistence.Index;
import javax.persistence.Table;

import com.latticeengines.domain.exposed.metadata.datastore.DataTemplate;

@Entity
@Table(name = "DataTemplate")
public class DataTemplateEntity extends BaseMultiTenantDocEntity<DataTemplate> {

}
