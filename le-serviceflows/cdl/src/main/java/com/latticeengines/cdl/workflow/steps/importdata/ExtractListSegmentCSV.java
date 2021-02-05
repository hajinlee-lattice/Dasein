package com.latticeengines.cdl.workflow.steps.importdata;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ATTR_LDC_DUNS;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.cdl.workflow.steps.merge.MatchUtils;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ParquetUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CreateDataTemplateRequest;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.metadata.ColumnField;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.ListSegment;
import com.latticeengines.domain.exposed.metadata.ListSegmentConfig;
import com.latticeengines.domain.exposed.metadata.MasterSchema;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.datastore.DataTemplate;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.domain.exposed.metadata.template.CSVAdaptor;
import com.latticeengines.domain.exposed.metadata.template.ImportFieldMapping;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.ImportListSegmentWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ExtractListSegmentCSVConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ExtractListSegmentCSVConfig;
import com.latticeengines.domain.exposed.spark.common.ConvertMatchResultConfig;
import com.latticeengines.domain.exposed.util.SegmentUtils;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.serviceflows.workflow.dataflow.BaseSparkStep;
import com.latticeengines.serviceflows.workflow.match.BulkMatchService;
import com.latticeengines.spark.exposed.job.cdl.ExtractListSegmentCSVJob;
import com.latticeengines.spark.exposed.job.common.ConvertMatchResultJob;

@Component("extractListSegmentCSV")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExtractListSegmentCSV
        extends BaseSparkStep<ExtractListSegmentCSVConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExtractListSegmentCSV.class);

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Inject
    private BatonService batonService;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private BulkMatchService bulkMatchService;

    @Value("${camille.zk.pod.id:Default}")
    protected String podId;

    @Value("${cdl.pa.use.directplus}")
    private boolean useDirectPlus;

    @Value("${pls.cdl.transform.cascading.partitions}")
    protected int cascadingPartitions;

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

    private String getDataCloudVersion() {
        return columnMetadataProxy.latestVersion("").getVersion();
    }

    @Override
    public void execute() {
        customerSpace = parseCustomerSpace(configuration);
        String tenantId = customerSpace.getTenantId();
        String segmentName = configuration.getSegmentName();
        String dataUnitName = getStringValueFromContext(ImportListSegmentWorkflowConfiguration.IMPORT_DATA_UNIT_NAME);
        S3DataUnit s3DataUnit = (S3DataUnit) dataUnitProxy.getByNameAndType(tenantId, dataUnitName, DataUnit.StorageType.S3);
        if (s3DataUnit != null) {
            MetadataSegment segment = segmentProxy.getListSegmentByName(tenantId, segmentName);
            if (SegmentUtils.hasListSegment(segment)) {
                ExtractListSegmentCSVConfig extractListSegmentCSVConfig = new ExtractListSegmentCSVConfig();
                extractListSegmentCSVConfig.setCsvAdaptor(segment.getListSegment().getCsvAdaptor());
                String hdfsPath = s3DataUnit.getLinkedHdfsPath();
                extractListSegmentCSVConfig.setInput(Collections.singletonList(getInputCSVDataUnit(hdfsPath, dataUnitName)));
                boolean needMatch = needMatch(segment.getListSegment());
                CSVAdaptor csvAdaptor = segment.getListSegment().getCsvAdaptor();
                Map<String, ImportFieldMapping> fieldMap = csvAdaptor.getImportFieldMappings().stream()
                        .collect(Collectors.toMap(importFieldMapping -> importFieldMapping.getFieldName(), importFieldMapping -> importFieldMapping));
                if (needMatch) {
                    setExtractListSegmentCSVConfig(extractListSegmentCSVConfig, 1,
                            fieldMap.keySet().stream().collect(Collectors.toList()), Collections.emptyList(), Collections.emptyMap());
                    SparkJobResult result = runSparkJob(ExtractListSegmentCSVJob.class, extractListSegmentCSVConfig);
                    HdfsDataUnit accountDataUnit = result.getTargets().get(0);
                    log.info("account data unit: {}.", JsonUtils.serialize(accountDataUnit));
                    MatchInput matchInput = constructMatchInput(accountDataUnit.getPath());
                    log.info("Bulk match input is {}", JsonUtils.serialize(matchInput));
                    MatchCommand command = bulkMatchService.match(matchInput, null);
                    log.info("Bulk match finished: {}", JsonUtils.serialize(command));
                    ConvertMatchResultConfig convertMatchResultConfig = getConvertMatchResultConfig(command);
                    result = runSparkJob(ConvertMatchResultJob.class, convertMatchResultConfig);
                    accountDataUnit = result.getTargets().get(0);
                    processImportResult(BusinessEntity.Account, accountDataUnit, ImportListSegmentWorkflowConfiguration.ACCOUNT_DATA_UNIT_NAME, fieldMap);
                    segment.setAccounts(accountDataUnit.getCount());
                } else {
                    Map<Integer, DataUnit.DataFormat> specialTargets = new HashMap<>();
                    specialTargets.put(0, DataUnit.DataFormat.PARQUET);
                    specialTargets.put(1, DataUnit.DataFormat.PARQUET);
                    setExtractListSegmentCSVConfig(extractListSegmentCSVConfig, 2, accountAttributes, contactAttributes, specialTargets);
                    SparkJobResult result = runSparkJob(ExtractListSegmentCSVJob.class, extractListSegmentCSVConfig);
                    HdfsDataUnit accountDataUnit = result.getTargets().get(0);
                    log.info("account data unit: {}.", JsonUtils.serialize(accountDataUnit));
                    processImportResult(BusinessEntity.Account, accountDataUnit, ImportListSegmentWorkflowConfiguration.ACCOUNT_DATA_UNIT_NAME, fieldMap);
                    HdfsDataUnit contactUnit = result.getTargets().get(1);
                    log.info("contact data unit: {}.", JsonUtils.serialize(contactUnit));
                    processImportResult(BusinessEntity.Contact, contactUnit, ImportListSegmentWorkflowConfiguration.CONTACT_DATA_UNIT_NAME, fieldMap);
                    segment.setAccounts(accountDataUnit.getCount());
                    segment.setContacts(contactUnit.getCount());
                }
                segment.setCountsOutdated(false);
                segmentProxy.createOrUpdateListSegment(tenantId, segment);
            } else {
                throw new RuntimeException(String.format("Can't find segment by name {}.", segmentName));
            }
        } else {
            throw new RuntimeException(String.format("S3 data unit {} doesn't exist.", dataUnitName));
        }
    }

    private void setExtractListSegmentCSVConfig(ExtractListSegmentCSVConfig extractListSegmentCSVConfig, int targetNumber,
                                                List<String> accountAttributes, List<String> contactAttributes, Map<Integer, DataUnit.DataFormat> specialTargets) {
        extractListSegmentCSVConfig.setAccountAttributes(accountAttributes);
        extractListSegmentCSVConfig.setContactAttributes(contactAttributes);
        extractListSegmentCSVConfig.setTargetNums(targetNumber);
        extractListSegmentCSVConfig.setSpecialTargets(specialTargets);
    }

    private ConvertMatchResultConfig getConvertMatchResultConfig(MatchCommand command) {
        String outputDir = PathUtils.toParquetOrAvroDir(command.getResultLocation());
        ConvertMatchResultConfig convertMatchResultConfig = new ConvertMatchResultConfig();
        Map<Integer, DataUnit.DataFormat> specialTargets = new HashMap<>();
        specialTargets.put(0, DataUnit.DataFormat.PARQUET);
        convertMatchResultConfig.setSpecialTargets(specialTargets);
        HdfsDataUnit input = new HdfsDataUnit();
        input.setPath(outputDir);
        convertMatchResultConfig.setInput(Lists.newArrayList(input));
        Map<String, String> names = new HashMap<>();
        names.put(ATTR_LDC_DUNS, InterfaceName.AccountId.name());
        convertMatchResultConfig.setDisplayNames(names);
        return convertMatchResultConfig;
    }

    private MatchInput getBaseMatchInput(String avroDir) {
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(customerSpace.getTenantId()));
        matchInput.setExcludePublicDomain(false);
        matchInput.setPublicDomainAsNormalDomain(false);
        matchInput.setDataCloudVersion(getDataCloudVersion());
        matchInput.setSkipKeyResolution(true);
        matchInput.setUseDnBCache(true);
        matchInput.setUseRemoteDnB(true);
        matchInput.setMatchDebugEnabled(false);
        matchInput.setUseDirectPlus(useDirectPlus);
        matchInput.setSplitsPerBlock(cascadingPartitions * 10);
        AvroInputBuffer inputBuffer = new AvroInputBuffer();
        inputBuffer.setAvroDir(avroDir);
        matchInput.setInputBuffer(inputBuffer);
        return matchInput;
    }

    protected List<String> getSystemIds(BusinessEntity entity) {
        Map<String, List<String>> systemIdMap = configuration.getSystemIdMaps();
        if (MapUtils.isEmpty(systemIdMap)) {
            return Collections.emptyList();
        }
        return systemIdMap.getOrDefault(entity.name(), Collections.emptyList());
    }

    private MatchInput constructMatchInput(String avroDir) {
        Boolean isCDLTenant = configuration.isCDLTenant();
        MatchInput matchInput = getBaseMatchInput(avroDir);
        boolean entityMatchEnabled = batonService.isEntityMatchEnabled(customerSpace);
        if (BooleanUtils.isTrue(isCDLTenant) && entityMatchEnabled) {
            matchInput.setOperationalMode(OperationalMode.ENTITY_MATCH);
            matchInput.setTargetEntity(Account.name());
            matchInput.setAllocateId(false);
            matchInput.setOutputNewEntities(false);
            matchInput.setIncludeLineageFields(false);
            matchInput.setPredefinedSelection(ColumnSelection.Predefined.ID);
            MatchInput.EntityKeyMap entityKeyMap = new MatchInput.EntityKeyMap();
            Set<String> inputFields = getInputFields(avroDir);
            entityKeyMap.setKeyMap(MatchUtils.getAccountMatchKeysAccount(inputFields, getSystemIds(BusinessEntity.Account), false, null));
            Map<String, MatchInput.EntityKeyMap> entityKeyMaps = new HashMap<>();
            entityKeyMaps.put(Account.name(), entityKeyMap);
            matchInput.setEntityKeyMaps(entityKeyMaps);
        } else {
            matchInput.setOperationalMode(OperationalMode.LDC_MATCH);
            matchInput.setTargetEntity(BusinessEntity.LatticeAccount.name());
            matchInput.setRequestSource(MatchRequestSource.ENRICHMENT);
            matchInput.setDataCloudOnly(true);
            Set<String> inputFields = getInputFields(avroDir);
            Map<MatchKey, List<String>> keyMap = MatchUtils.getAccountMatchKeysAccount(inputFields, null, false, null);
            matchInput.setKeyMap(keyMap);
            matchInput.setCustomSelection(getColumnSelection());
            matchInput.setPartialMatchEnabled(true);
        }
        return matchInput;
    }

    private ColumnSelection getColumnSelection() {
        List<Column> columns = Collections.singletonList(new Column(ATTR_LDC_DUNS));
        ColumnSelection columnSelection = new ColumnSelection();
        columnSelection.setColumns(columns);
        return columnSelection;
    }

    private Set<String> getInputFields(String avroDir) {
        String avroGlob = PathUtils.toAvroGlob(avroDir);
        Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroGlob);
        return schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toSet());
    }

    private boolean needMatch(ListSegment listSegment) {
        ListSegmentConfig listSegmentConfig = listSegment.getConfig();
        if (listSegmentConfig != null) {
            return listSegmentConfig.isNeedToMatch();
        } else {
            return false;
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
                        ImportFieldMapping phoneMapping = fieldMap.get(ExtractListSegmentCSVConfiguration.Direct_Phone);
                        if (phoneMapping != null) {
                            attribute.setDisplayName(phoneMapping.getUserFieldName());
                        }
                    } else {
                        ImportFieldMapping importFieldMapping = fieldMap.get(field.name());
                        if (importFieldMapping != null) {
                            attribute.setDisplayName(fieldMap.get(field.name()).getUserFieldName());
                        }
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
        if (BusinessEntity.Account.equals(entity)) {
            S3DataUnit preMasterDataUnit = (S3DataUnit) dataUnitProxy.getByDataTemplateIdAndRole(tenantId, templateId, DataUnit.Role.Master);
            if (preMasterDataUnit != null) {
                String name = preMasterDataUnit.getName();
                log.info("previous account athena data unit name is " + name);
                putStringValueInContext(ImportListSegmentWorkflowConfiguration.PREVIOUS_ACCOUNT_ATHENA_UNIT_NAME, name);
            }
        }
        S3DataUnit s3DataUnit = toS3DataUnit(hdfsDataUnit, entity, templateId,
                Lists.newArrayList(DataUnit.Role.Master, DataUnit.Role.Snapshot));
        dataUnitProxy.create(tenantId, s3DataUnit);
        putStringValueInContext(contextKey, s3DataUnit.getName());
    }
}
