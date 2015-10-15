package com.latticeengines.domain.exposed.propdata.collection;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "FeatureArchiveProgress", uniqueConstraints = { @UniqueConstraint(columnNames = { "RootOperationUID" }) })
public class FeatureArchiveProgress extends ArchiveProgressBase implements HasPid {
}

