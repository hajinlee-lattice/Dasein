package com.latticeengines.cdl.workflow.steps.importdata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ParquetUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CreateDataTemplateRequest;
import com.latticeengines.domain.exposed.metadata.ColumnField;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MasterSchema;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.datastore.DataTemplate;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.domain.exposed.metadata.template.CSVAdaptor;
import com.latticeengines.domain.exposed.metadata.template.ImportFieldMapping;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.ImportListSegmentWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ExtractListSegmentCSVConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ExtractListSegmentCSVConfig;
import com.latticeengines.domain.exposed.util.SegmentUtils;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.serviceflows.workflow.dataflow.BaseSparkStep;
import com.latticeengines.spark.exposed.job.cdl.ExtractListSegmentCSVJob;

@Component("extractListSegmentCSV")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExtractListSegmentCSV
        extends BaseSparkStep<ExtractListSegmentCSVConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExtractListSegmentCSV.class);

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Value("${camille.zk.pod.id:Default}")
    protected String podId;

    private final List<String> accountAttributes = Lists.newArrayList(InterfaceName.CompanyName.name(),
            InterfaceName.Address_Street_1.name(), InterfaceName.Address_Street_2.name(), InterfaceName.City.name(),
            InterfaceName.State.name(), InterfaceName.Country.name(), InterfaceName.PostalCode.name(),
            InterfaceName.DUNS.name(), InterfaceName.PhoneNumber.name(), "SFDC_ACCOUNT_ID",
            InterfaceName.Website.name(), InterfaceName.Industry.name(), "user_us_8_digit_sic_code",
            InterfaceName.AccountId.name(), "user_employees", "user_direct_marketing_status");

    private final List<String> contactAttributes = Lists.newArrayList(InterfaceName.AccountId.name(), "SFDC_CONTACT_ID",
            InterfaceName.ContactName.name(), InterfaceName.Email.name(), InterfaceName.FirstName.name(), InterfaceName.LastName.name(),
            ExtractListSegmentCSVConfiguration.Direct_Phone, InterfaceName.Title.name(), InterfaceName.ContactId.name(),
            "user_level_name", "user_job_function", InterfaceName.GCA_ID.name());

    @Override
    protected CustomerSpace parseCustomerSpace(ExtractListSegmentCSVConfiguration stepConfiguration) {
        return stepConfiguration.getCustomerSpace();
    }

    @Override
    public void execute() {
        customerSpace = parseCustomerSpace(configuration);
        String tenantId = customerSpace.getTenantId();
        String segmentName = configuration.getSegmentName();
        String dataUnitName = getStringValueFromContext(ImportListSegmentWorkflowConfiguration.IMPORT_DATA_UNIT_NAME);
        S3DataUnit s3DataUnit = (S3DataUnit) dataUnitProxy.getByNameAndType(tenantId, dataUnitName, DataUnit.StorageType.S3);
        if (s3DataUnit != null) {
            ExtractListSegmentCSVConfig extractListSegmentCSVConfig = new ExtractListSegmentCSVConfig();
            extractListSegmentCSVConfig.setAccountAttributes(accountAttributes);
            extractListSegmentCSVConfig.setContactAttributes(contactAttributes);
            MetadataSegment segment = segmentProxy.getListSegmentByName(tenantId, segmentName);
            if (SegmentUtils.hasListSegment(segment)) {
                extractListSegmentCSVConfig.setCsvAdaptor(segment.getListSegment().getCsvAdaptor());
                String hdfsPath = s3DataUnit.getLinkedHdfsPath();
                extractListSegmentCSVConfig.setInput(Collections.singletonList(getInputCSVDataUnit(hdfsPath, dataUnitName)));
                extractListSegmentCSVConfig.setTargetNums(2);
                Map<Integer, DataUnit.DataFormat> specialTargets = new HashMap<>();
                specialTargets.put(0, DataUnit.DataFormat.PARQUET);
                specialTargets.put(1, DataUnit.DataFormat.PARQUET);
                extractListSegmentCSVConfig.setSpecialTargets(specialTargets);
                SparkJobResult result = runSparkJob(ExtractListSegmentCSVJob.class, extractListSegmentCSVConfig);
                HdfsDataUnit accountDataUnit = result.getTargets().get(0);
                log.info("account info data unit: {}.", JsonUtils.serialize(accountDataUnit));
                CSVAdaptor csvAdaptor = segment.getListSegment().getCsvAdaptor();
                Map<String, ImportFieldMapping> fieldMap = csvAdaptor.getImportFieldMappings().stream()
                        .collect(Collectors.toMap(importFieldMapping -> importFieldMapping.getFieldName(), importFieldMapping -> importFieldMapping));
                processImportResult(BusinessEntity.Account, accountDataUnit,
                        ImportListSegmentWorkflowConfiguration.ACCOUNT_DATA_UNIT_NAME, fieldMap);
                HdfsDataUnit contactUnit = result.getTargets().get(1);
                log.info("contactUnit info data unit: {}.", JsonUtils.serialize(accountDataUnit));
                processImportResult(BusinessEntity.Contact, contactUnit,
                        ImportListSegmentWorkflowConfiguration.CONTACT_DATA_UNIT_NAME, fieldMap);
                //update segment count
                segment.setAccounts(accountDataUnit.getCount());
                segment.setContacts(contactUnit.getCount());
                segment.setCountsOutdated(false);
                segmentProxy.createOrUpdateListSegment(tenantId, segment);
            } else {
                throw new RuntimeException(String.format("Can't find segment by name {}.", segmentName));
            }
        } else {
            throw new RuntimeException(String.format("S3 data unit {} doesn't exist.", dataUnitName));
        }
    }

    private HdfsDataUnit getInputCSVDataUnit(String path, String name) {
        HdfsDataUnit unit = new HdfsDataUnit();
        unit.setName(name);
        String hdfsPath = path;
        if (!hdfsPath.endsWith(".csv")) {
            hdfsPath = PathUtils.toCSVGlob(hdfsPath);
        }
        unit.setPath(hdfsPath);
        unit.setDataFormat(DataUnit.DataFormat.CSV);
        return unit;
    }

    private MasterSchema getSchema(HdfsDataUnit hdfsDataUnit, BusinessEntity entity, Map<String, ImportFieldMapping> fieldMap) {
        String path = hdfsDataUnit.getPath();
        MasterSchema masterSchema = new MasterSchema();
        List<ColumnField> attributes = new ArrayList<>();
        try {
            List<String> matchedFiles = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsDataUnit.getPath(),
                    (HdfsUtils.HdfsFilenameFilter) filename -> filename.endsWith(".parquet"));
            if (CollectionUtils.isNotEmpty(matchedFiles)) {
                Schema parquetSchema = ParquetUtils.getAvroSchema(yarnConfiguration, matchedFiles.get(0));
                for (Schema.Field field : parquetSchema.getFields()) {
                    ColumnField attribute = new ColumnField();
                    attribute.setAttrName(field.name());
                    if (BusinessEntity.Contact.equals(entity) && InterfaceName.PhoneNumber.name().equals(field.name())) {
                        attribute.setDisplayName(fieldMap.get(ExtractListSegmentCSVConfiguration.Direct_Phone).getUserFieldName());
                    } else {
                        attribute.setDisplayName(fieldMap.get(field.name()).getUserFieldName());
                    }
                    attributes.add(attribute);
                }
            } else {
                log.info("Did not find any parquet files under folder " + path + ".");
                throw new RuntimeException("Did not find any parquet files under folder " + path + ".");
            }
        } catch (Exception ex) {
            log.error("Unexpected exception: ", ex);
            throw new RuntimeException("Did not find any parquet files under folder " + path + ".");
        }
        masterSchema.setFields(attributes);
        List<String> primaryKey = new ArrayList<>();
        if (entity != null && entity == BusinessEntity.Account) {
            primaryKey.add(InterfaceName.AccountId.name());
        } else if (entity != null && entity == BusinessEntity.Contact) {
            primaryKey.add(InterfaceName.ContactId.name());
        }
        masterSchema.setPrimaryKey(primaryKey);
        return masterSchema;
    }

    private CreateDataTemplateRequest createRequest(String templateKey, MasterSchema schema) {
        CreateDataTemplateRequest request = new CreateDataTemplateRequest();
        request.setTemplateKey(templateKey);
        DataTemplate dataTemplate = new DataTemplate();
        dataTemplate.setName(request.getTemplateKey());
        dataTemplate.setMasterSchema(schema);
        request.setDataTemplate(dataTemplate);
        return request;
    }

    private void processImportResult(BusinessEntity entity, HdfsDataUnit hdfsDataUnit, String contextKey, Map<String, ImportFieldMapping> fieldMap) {
        String tenantId = customerSpace.getTenantId();
        CreateDataTemplateRequest request = createRequest(entity.name(), getSchema(hdfsDataUnit, entity, fieldMap));
        String templateId = segmentProxy.createOrUpdateDataTemplate(tenantId, configuration.getSegmentName(), request);
        S3DataUnit s3DataUnit = toS3DataUnit(hdfsDataUnit, entity, templateId,
                Lists.newArrayList(DataUnit.Role.Master, DataUnit.Role.Snapshot));
        dataUnitProxy.create(tenantId, s3DataUnit);
        putStringValueInContext(contextKey, s3DataUnit.getName());
    }
}
