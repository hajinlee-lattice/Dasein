package com.latticeengines.domain.exposed.metadata;

import javax.persistence.Column;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Entity
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
@DiscriminatorValue("JDBC")
public class JdbcStorage extends StorageMechanism {

    public static enum DatabaseName {
        REDSHIFT, //
        MYSQL, //
        SQLSERVER
    }
    
    @Column(name = "DATABASE_NAME", nullable = true)
    private DatabaseName databaseName;

    public DatabaseName getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(DatabaseName databaseName) {
        this.databaseName = databaseName;
    }

}
