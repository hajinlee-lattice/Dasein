package com.latticeengines.domain.exposed.metadata;

import javax.persistence.Column;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Entity
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
@DiscriminatorValue("NoSQL")
public class NoSQLStorage extends StorageMechanism {

    @Column(name = "DATABASE_NAME", nullable = true)
    private DatabaseName databaseName;

    public DatabaseName getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(DatabaseName databaseName) {
        this.databaseName = databaseName;
    }

    public enum DatabaseName {
        DYNAMO
    }
}
