package com.latticeengines.cdl.dataflow.redshiftdb;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.RedshiftPublishDataFlowParameters;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.redshiftdb.exposed.utils.RedshiftUtils;

import cascading.avro.AvroScheme;
import cascading.jdbc.AWSCredentials;
import cascading.jdbc.RedshiftTableDesc;
import cascading.jdbc.RedshiftTap;
import cascading.jdbc.RedshiftFactory.CopyOption;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

@Component("redshiftPublishDataflow")
public class RedshiftPublishDataFlow extends TypesafeDataFlowBuilder<RedshiftPublishDataFlowParameters> {

    @Value("${aws.default.access.key}")
    private String awsAccessKey;

    @Value("${aws.default.secret.key.encrypted}")
    private String awsSecretKey;

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${redshift.jdbc.url}")
    private String redshiftJdbcUrl;

    @Value("${redshift.user}")
    private String redshiftUsername;

    @Value("${redshift.password.encrypted}")
    private String redshiftPassword;

    @Value("${redshift.app.database}")
    private String redshiftDB;

    protected Tap<?, ?, ?> createSink(String lastOperator, String targetPath) {

        String flowName = dataFlowCtx.getProperty(DataFlowProperty.FLOWNAME, String.class);
        Schema schema = getSchema(flowName, lastOperator, dataFlowCtx);

        RedshiftTableConfiguration redshiftTableConfig = dataFlowCtx.getProperty(DataFlowProperty.PARAMETERS,
                RedshiftPublishDataFlowParameters.class).redshiftTableConfig;

        String targetRedshiftTable = redshiftTableConfig.getTableName();
        String distributionKey = redshiftTableConfig.getDistKey();
        redshiftJdbcUrl = redshiftJdbcUrl + "/" + redshiftDB;

        String[] fieldNames = schema.getFields().stream().map(f -> f.name()).toArray(size -> new String[size]);
        String[] fieldTypes = schema.getFields().stream() //
                .map(f -> RedshiftUtils.getSQLType(f, false)).toArray(size -> new String[size]);
        String[] sortKeys = redshiftTableConfig.getSortKeys()
                .toArray(new String[redshiftTableConfig.getSortKeys().size()]);

        RedshiftTableDesc redshiftTableDesc = new RedshiftTableDesc(targetRedshiftTable, fieldNames, fieldTypes,
                redshiftTableConfig.getDistStyle().getName(), distributionKey,
                redshiftTableConfig.getSortKeyType().getName(), sortKeys);

        AvroScheme scheme = new AvroScheme(schema);
        if (enforceGlobalOrdering()) {
            scheme.setNumSinkParts(1);
        }
        Map<CopyOption, String> options = createCopyOptions(redshiftTableConfig);

        RedshiftAvroScheme redshiftScheme = new RedshiftAvroScheme(new Fields(fieldNames), redshiftTableDesc, scheme,
                options);

        AWSCredentials awsCredentials = new AWSCredentials(awsAccessKey, awsSecretKey);
        String s3Path = "s3n://" + s3Bucket + "/" + RedshiftUtils.AVRO_STAGE + "/" + leStack + "/"
                + targetRedshiftTable;

        Tap<?, ?, ?> sink = new RedshiftTap(redshiftJdbcUrl, redshiftUsername, redshiftPassword, scheme, s3Path,
                awsCredentials, redshiftTableDesc, redshiftScheme, SinkMode.REPLACE, true, false);

        return sink;
    }

    @Override
    public Node construct(RedshiftPublishDataFlowParameters parameters) {
        Node source = addSource(parameters.sourceTable);
        return source;
    }

    private Map<CopyOption, String> createCopyOptions(RedshiftTableConfiguration redshiftTableConfig) {
        String jsonPathPrefix = redshiftTableConfig.getJsonPathPrefix();
        Map<CopyOption, String> options = new HashMap<>();
        options.put(CopyOption.FORMAT, String.format("AVRO \'%s\'", jsonPathPrefix));
        return options;
    }

}
