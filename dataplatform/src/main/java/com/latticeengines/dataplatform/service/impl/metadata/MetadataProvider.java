package com.latticeengines.dataplatform.service.impl.metadata;

import java.sql.Connection;

import com.latticeengines.dataplatform.exposed.domain.DbCreds;

public interface MetadataProvider {

    String getName();

    Connection getConnection(DbCreds creds);

    String getType(String dbType);

    String getDefaultValue(String dbType);

}
