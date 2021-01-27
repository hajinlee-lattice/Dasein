package com.latticeengines.cdl.workflow.steps.integration;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.AggregationHistory;
import com.latticeengines.domain.exposed.cdl.integration.BrokerReference;
import com.latticeengines.domain.exposed.cdl.integration.InboundConnectionType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnField;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MasterSchema;
import com.latticeengines.domain.exposed.metadata.datastore.DataTemplate;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.integration.BrokerDataFullLoadConfiguration;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.proxy.exposed.cdl.AggregationHistoryProxy;
import com.latticeengines.proxy.exposed.cdl.InboundConnectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataTemplateProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("brokerDataFullLoad")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class BrokerDataFullLoad extends BaseWorkflowStep<BrokerDataFullLoadConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(BrokerDataFullLoad.class);

    @Inject
    private DataTemplateProxy dataTemplateProxy;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Inject
    private InboundConnectionProxy inboundConnectionProxy;

    @Inject
    private AggregationHistoryProxy aggregationHistoryProxy;

    @Inject
    private S3Service s3Service;

    private long defaultSize = 10000;
    private String prefix = "enterprise_integration/";
    private String fileDir = "/tmp/";

    @Value("${camille.zk.pod.id}")
    protected String podId;

    @Override
    public void execute() {
        String tenantId = configuration.getCustomerSpace().getTenantId();
        InboundConnectionType inboundConnectionType = configuration.getInboundConnectionType();
        String sourceId = configuration.getSourceId();
        BrokerReference brokerReference = inboundConnectionProxy.getBrokerReference(tenantId,
                createBrokerReference(inboundConnectionType, sourceId));
        if (brokerReference == null) {
            throw new LedpException(LedpCode.LEDP_40100, new String[]{sourceId});
        }
        DataTemplate dataTemplate = createDataTemplate(tenantId, brokerReference);
        String templateId = dataTemplateProxy.create(tenantId, dataTemplate);
        BusinessEntity entity = BusinessEntity.valueOf(brokerReference.getDocumentType());
        S3DataUnit s3DataUnit = createS3DataUnit(tenantId, entity, templateId);
        s3DataUnit = (S3DataUnit) dataUnitProxy.create(tenantId, s3DataUnit, false);
        generateInitData(s3DataUnit, brokerReference);
        AggregationHistory aggregationHistory = new AggregationHistory();
        aggregationHistory.setLastSyncTime(configuration.getEndTime());
        aggregationHistory.setDataStreamId(templateId);
        aggregationHistory.setSourceId(sourceId);
        aggregationHistoryProxy.create(tenantId, aggregationHistory);
        brokerReference.setActive(true);
        brokerReference.setDataStreamId(templateId);
        inboundConnectionProxy.updateBroker(tenantId, brokerReference);
    }

    private DataTemplate createDataTemplate(String tenantId, BrokerReference brokerReference) {
        DataTemplate dataTemplate = new DataTemplate();
        dataTemplate.setTenant(tenantId);
        dataTemplate.setName(DATA_STREAM);
        dataTemplate.setMasterSchema(createMaterSchema(brokerReference));
        return dataTemplate;
    }

    private MasterSchema createMaterSchema(BrokerReference brokerReference) {
        MasterSchema masterSchema = new MasterSchema();
        List<String> primaryKeys = new ArrayList<>();
        String documentType = brokerReference.getDocumentType();
        List<String> selectedFields = brokerReference.getSelectedFields();
        BusinessEntity entity = BusinessEntity.valueOf(documentType);
        switch (entity) {
            case Account:
                primaryKeys.add(InterfaceName.AccountId.name());
                break;
            case Contact:
                primaryKeys.add(InterfaceName.ContactId.name());
                break;
            default:
                throw new RuntimeException(String.format("Unsupported entity type: %s", entity.name()));
        }
        masterSchema.setPrimaryKey(primaryKeys);
        masterSchema.setFields(selectedFields.stream().map(key -> {
            ColumnField columnField = new ColumnField();
            columnField.setAttrName(key);
            return columnField;
        }).collect(Collectors.toList()));
        return masterSchema;
    }

    private BrokerReference createBrokerReference(InboundConnectionType inboundConnectionType, String sourceId) {
        BrokerReference brokerReference = new BrokerReference();
        brokerReference.setConnectionType(inboundConnectionType);
        brokerReference.setSourceId(sourceId);
        return brokerReference;
    }

    private S3DataUnit createS3DataUnit(String tenantId, BusinessEntity entity, String templateId) {
        HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder();
        String bucket = configuration.getBucket();
        S3DataUnit s3DataUnit = new S3DataUnit();
        String dataUnitName = NamingUtils.uuid(entity.name());
        s3DataUnit.setTenant(tenantId);
        s3DataUnit.setName(dataUnitName);
        s3DataUnit.setCount(defaultSize);
        String tgtPath = pathBuilder.getHdfsAtlasDataUnitPrefix(podId, tenantId, templateId, dataUnitName);
        s3DataUnit.setLinkedHdfsPath(tgtPath);
        s3DataUnit.setRoles(Lists.newArrayList(DataUnit.Role.Snapshot));
        s3DataUnit.setBucket(bucket);
        s3DataUnit.setDataFormat(DataUnit.DataFormat.AVRO);
        s3DataUnit.setDataTemplateId(templateId);
        String key = pathBuilder.getS3AtlasDataUnitPrefix(bucket, tenantId, templateId, dataUnitName);
        key = key.substring(key.indexOf(bucket) + bucket.length() + 1);
        s3DataUnit.setPrefix(key);
        return s3DataUnit;
    }

    private void generateInitData(S3DataUnit s3DataUnit, BrokerReference brokerReference) {
        String fileName = s3DataUnit.getName() + ".avro";
        String separator = HdfsToS3PathBuilder.PATH_SEPARATOR;
        String key = s3DataUnit.getPrefix() + separator + fileName;
        File file = new File(fileName);
        uploadFileToS3(s3DataUnit.getName(), file, brokerReference.getSelectedFields(), s3DataUnit.getBucket(), key);
    }

    private Schema getSchema(List<String> fieldNames, String name) {
        Schema schema = Schema
                .createUnion(Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)));
        ArrayList<Schema.Field> appendList = new ArrayList();
        for (String fieldName : fieldNames) {
            Schema.Field field = new Schema.Field(fieldName, schema, null, "");
            appendList.add(field);
        }
        return Schema.createRecord(name, null, null, false, appendList);
    }

    private void uploadFileToS3(String name, File file, List<String> fieldNames, String bucket, String key) {
        try {
            boolean successCreated = true;
            Schema schema = getSchema(fieldNames, name);
            try (DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
                writer.setCodec(CodecFactory.snappyCodec());
                writer.create(schema, new FileOutputStream(file));
                for (int i = 0; i < defaultSize; i++) {
                    GenericRecord record = new GenericData.Record(schema);
                    for (String fieldName : fieldNames) {
                        record.put(fieldName, getFieldValue(fieldName));
                    }
                    writer.append(record);
                }
            } catch (Exception e) {
                successCreated = false;
                log.error("Error happened when create csv file: ", e);
            }
            if (successCreated) {
                s3Service.uploadLocalFile(bucket, key, file, true);
            }
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    private String getFieldValue(String fieldName) {
        switch (fieldName) {
            case "AccountId":
            case "ContactId":
                return UUID.randomUUID().toString();
            case "CompanyName":
                return "IBM Corporation";
            case "City":
                return "Boston";
            case "Country":
                return "United States";
            case "PhoneNumber":
                return "+1 (608) 395-1234";
            case "Industry":
                return "Business Services";
            case "Website":
                return "www.microstrategy.com";
            case "Email":
                return "testuser@lattice-engines.com";
            case "FirstName":
                return "testF";
            case "LastName":
                return "testL";
            case "Title":
                return "Senior IT Security Officer";
            default:
                throw new RuntimeException("Unknown field name");
        }
    }
}
