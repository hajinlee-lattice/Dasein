package com.latticeengines.db.exposed.schemagen;

import java.sql.Types;

import org.hibernate.dialect.SQLServer2008Dialect;

public class LeSQLServer2008Dialect extends SQLServer2008Dialect {

    // same as in the super class
    private static final int MAX_LENGTH = 8000;
    
    public LeSQLServer2008Dialect() {                
        registerColumnType(Types.VARCHAR, "nvarchar(max)" );
        // this is a compatibility fix for mysql.  domain object contain 65535 size to fit mysql.
        // for sqlserver, generate nvarchar(max)
        registerColumnType(Types.VARCHAR, 65535, "nvarchar(max)");
        registerColumnType(Types.CLOB, "nvarchar(MAX)" );
        registerColumnType(Types.LONGVARCHAR, "varchar(MAX)" );
        registerColumnType( Types.VARCHAR, MAX_LENGTH, "nvarchar($l)" );
        
        
    }
    
}
