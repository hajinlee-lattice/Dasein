//package com.latticeengines.sqoop.service.impl.metadata;
//
//import java.io.IOException;
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.SQLException;
//import java.util.List;
//
//import org.apache.avro.Schema;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.sqoop.manager.DefaultManagerFactory;
//import org.apache.sqoop.orm.AvroSchemaGenerator;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.jdbc.core.JdbcTemplate;
//import org.springframework.jdbc.support.JdbcUtils;
//import org.springframework.stereotype.Component;
//
//import com.cloudera.sqoop.ConnFactory;
//import com.cloudera.sqoop.SqoopOptions;
//import com.cloudera.sqoop.manager.ConnManager;
//import com.cloudera.sqoop.metastore.JobData;
//import com.latticeengines.db.exposed.service.DbMetadataService;
//import com.latticeengines.domain.exposed.exception.LedpCode;
//import com.latticeengines.domain.exposed.exception.LedpException;
//import com.latticeengines.domain.exposed.modeling.DbCreds;
//
//@SuppressWarnings("deprecation")
//@Component("sqoopMetadataProvider")
//public class SqoopMetadataProvider {
//
//    @Autowired
//    private DbMetadataService dbMetadataService;
//    
//    private AvroSchemaGenerator avroSchemaGenerator;
//
//    public ConnManager getConnectionManager(SqoopOptions options) {
//        JobData data = new JobData(options, null);
//        return new DefaultManagerFactory().accept(data);
//    }
//
//    public Schema getSchema(DbCreds dbCreds, String tableName) {
//        SqoopOptions options = new SqoopOptions();
//        options.setConnectString(dbMetadataService.getConnectionString(dbCreds));
//
//        if (!StringUtils.isEmpty(dbCreds.getDriverClass())) {
//            options.setDriverClassName(dbCreds.getDriverClass());
//        }
//
//        ConnManager connManager = getConnectionManager(options);
//        avroSchemaGenerator = new AvroSchemaGenerator(options, connManager, tableName);
//        try {
//            return avroSchemaGenerator.generate();
//        } catch (IOException e) {
//            return null;
//        }
//    }
//
//}
