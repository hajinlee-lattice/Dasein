package com.latticeengines.cdl.dataflow.redshiftdb;

import java.util.Properties;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.domain.exposed.dataflow.flows.RedshiftDataFlowParameters;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.redshiftdb.exposed.utils.RedshiftUtils;

import cascading.avro.AvroScheme;
import cascading.jdbc.AWSCredentials;
import cascading.jdbc.RedshiftTableDesc;
import cascading.jdbc.RedshiftTap;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

@Component("redshiftExportDataflow")
public class RedshiftExportDataFlow extends TypesafeDataFlowBuilder<RedshiftDataFlowParameters> {

    @Value("${aws.default.access.key.encrypted}")
    private String awsAccessKey;

    @Value("${aws.default.secret.key.encrypted}")
    private String awsSecretKey;

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${redshift.user}")
    private String redshiftUsername;

    @Value("${redshift.password.encrypted}")
    private String redshiftPassword;

    @Value("${redshift.app.database}")
    private String redshiftDB;

    protected Tap<?, ?, ?> createSink(String lastOperator, String targetPath) {

        String flowName = dataFlowCtx.getProperty(DataFlowProperty.FLOWNAME, String.class);
        Schema schema = getSchema(flowName, lastOperator, dataFlowCtx);

        RedshiftTableConfiguration redshiftTableConfig = dataFlowCtx
                .getProperty(RedshiftTableConfiguration.class.getSimpleName(), RedshiftTableConfiguration.class);

        String targetRedshiftTable = redshiftTableConfig.getTableName();
        String distributionKey = redshiftTableConfig.getDistKey();
        String redshiftJdbcUrl = "jdbc:redshift://redshift.cegcmwpsftfz.us-east-1.redshift.amazonaws.com:5439/"
                + redshiftDB;

        String[] fieldNames = schema.getFields().stream().map(a -> a.name()).toArray(size -> new String[size]);
        String[] fieldTypes = schema.getFields().stream() //
                .map(a -> RedshiftUtils.getSQLType(a.schema())).toArray(size -> new String[size]);
        String[] sortKeys = redshiftTableConfig.getSortKeys().toArray(new String[0]);

        RedshiftTableDesc redshiftTableDesc = new RedshiftTableDesc(targetRedshiftTable, fieldNames, fieldTypes,
                distributionKey, sortKeys);

        AvroScheme scheme = new AvroScheme(schema);
        if (enforceGlobalOrdering()) {
            scheme.setNumSinkParts(1);
        }
        String jsonPathPrefix = redshiftTableConfig.getJsonPathPrefix();
        RedshiftAvroScheme redshiftScheme = new RedshiftAvroScheme(new Fields(fieldNames), redshiftTableDesc, scheme,
                jsonPathPrefix);

        AWSCredentials awsCredentials = new AWSCredentials(awsAccessKey, awsSecretKey);
        Properties jobProperties = dataFlowCtx.getProperty(DataFlowProperty.JOBPROPERTIES, Properties.class);
        jobProperties.setProperty("fs.s3n.awsAccessKeyId", awsAccessKey);
        jobProperties.setProperty("fs.s3n.awsSecretAccessKey", awsSecretKey);

        String tempPath = "s3n://" + s3Bucket + "/" + RedshiftUtils.AVRO_STAGE + "/" + leStack + "/"
                + targetRedshiftTable;
        Tap<?, ?, ?> sink = new RedshiftTap(redshiftJdbcUrl, redshiftUsername, redshiftPassword, scheme, tempPath,
                awsCredentials, redshiftTableDesc, redshiftScheme, SinkMode.REPLACE, true, false);

        return sink;
    }

    @Override
    public Node construct(RedshiftDataFlowParameters parameters) {
        Node source = addSource(parameters.sourceTable);
        return source;
    }

}
