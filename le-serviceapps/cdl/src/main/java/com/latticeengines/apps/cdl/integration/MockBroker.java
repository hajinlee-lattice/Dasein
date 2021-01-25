package com.latticeengines.apps.cdl.integration;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.latticeengines.apps.cdl.service.InboundConnectionService;
import com.latticeengines.apps.cdl.service.MockBrokerInstanceService;
import com.latticeengines.common.exposed.util.CronUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.IngestionScheduler;
import com.latticeengines.domain.exposed.cdl.MockBrokerInstance;
import com.latticeengines.domain.exposed.cdl.integration.BrokerFullLoadRequest;
import com.latticeengines.domain.exposed.cdl.integration.BrokerReference;
import com.latticeengines.domain.exposed.cdl.integration.InboundConnectionType;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("mockBroker")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MockBroker extends BaseBroker {

    private static final Logger log = LoggerFactory.getLogger(MockBroker.class);

    private final List<String> accountAttributes = Lists.newArrayList(InterfaceName.AccountId.name(), InterfaceName.CompanyName.name(),
            InterfaceName.City.name(), InterfaceName.Country.name(), InterfaceName.PhoneNumber.name(), InterfaceName.Website.name(), InterfaceName.Industry.name());

    private final List<String> contactAttributes = Lists.newArrayList(InterfaceName.AccountId.name(), InterfaceName.ContactId.name(), InterfaceName.Email.name(),
            InterfaceName.FirstName.name(), InterfaceName.LastName.name(), InterfaceName.Title.name());

    @Inject
    private MockBrokerInstanceService mockBrokerInstanceService;

    @Inject
    private InboundConnectionService inboundConnectionService;

    public MockBroker(BrokerReference brokerReference) {
        super(brokerReference);
    }

    @Override
    public void pause() {
        setActive(false);
    }

    @Override
    public void schedule(IngestionScheduler scheduler) {
        MockBrokerInstance mockBrokerInstance = mockBrokerInstanceService.findBySourceId(sourceId);
        if (mockBrokerInstance != null) {
            CustomerSpace customerSpace = CustomerSpace.parse(MultiTenantContext.getShortTenantId());
            IngestionScheduler preScheduler = mockBrokerInstance.getIngestionScheduler();
            mockBrokerInstance.setIngestionScheduler(scheduler);
            mockBrokerInstance.setActive(false);
            mockBrokerInstance.setDataStreamId(null);
            BrokerFullLoadRequest request = new BrokerFullLoadRequest();
            request.setInboundConnectionType(InboundConnectionType.Mock);
            request.setSourceId(sourceId);
            request.setStartTime(scheduler.getStartTime());
            request.setBucket(s3Bucket);
            if (preScheduler != null) {
                request.setEndTime(getPreFireTime(preScheduler, scheduler).toDate());
            } else {
                request.setEndTime(getPreFireTime(scheduler).toDate());
            }
            long workflowPid = submitFullLoadWorkflow(customerSpace, request);
            mockBrokerInstance.setLastAggregationWorkflowId(workflowPid);
            mockBrokerInstanceService.createOrUpdate(mockBrokerInstance);
        } else {
            throw new RuntimeException(String.format("Can'd find mock instance by id %s.", sourceId));
        }
    }

    private DateTime getPreFireTime(IngestionScheduler old, IngestionScheduler current) {
        DateTime oldPreFireTime = getPreFireTime(old);
        DateTime currentPreFireTime = getPreFireTime(current);
        return oldPreFireTime.isBefore(currentPreFireTime) ? currentPreFireTime : oldPreFireTime;
    }


    private DateTime getPreFireTime(IngestionScheduler scheduler) {
        String cronExpression = scheduler.getCronExpression();
        return CronUtils.getPreviousFireTime(cronExpression);
    }

    @Override
    public void start() {
        setActive(true);
    }

    @Override
    public void update(BrokerReference brokerReference) {
        MockBrokerInstance mockBrokerInstance = new MockBrokerInstance();
        mockBrokerInstance.setSourceId(sourceId);
        mockBrokerInstance.setDataStreamId(brokerReference.getDataStreamId());
        mockBrokerInstance.setActive(brokerReference.getActive());
        mockBrokerInstanceService.createOrUpdate(mockBrokerInstance);
    }

    private void setActive(boolean active) {
        MockBrokerInstance mockBrokerInstance = mockBrokerInstanceService.findBySourceId(sourceId);
        if (mockBrokerInstance != null) {
            mockBrokerInstance.setActive(active);
            mockBrokerInstanceService.createOrUpdate(mockBrokerInstance);
        } else {
            throw new RuntimeException(String.format("Can'd find mock instance by id %s.", sourceId));
        }
    }

    @Override
    public List<String> listDocumentTypes() {
        return Lists.newArrayList(BusinessEntity.Account.name(), BusinessEntity.Contact.name());
    }

    @Override
    public List<ColumnMetadata> describeDocumentType(String documentType) {
        if (StringUtils.isNotEmpty(documentType)) {
            BusinessEntity entity = BusinessEntity.valueOf(documentType);
            switch (entity) {
                case Account:
                    return accountAttributes.stream().map(attr -> {
                        ColumnMetadata columnMetadata = new ColumnMetadata();
                        columnMetadata.setAttrName(attr);
                        columnMetadata.setCategory(Category.ACCOUNT_ATTRIBUTES);
                        columnMetadata.setEntity(entity);
                        return columnMetadata;
                    }).collect(Collectors.toList());
                case Contact:
                    return contactAttributes.stream().map(attr -> {
                        ColumnMetadata columnMetadata = new ColumnMetadata();
                        columnMetadata.setAttrName(attr);
                        columnMetadata.setCategory(Category.CONTACT_ATTRIBUTES);
                        columnMetadata.setEntity(entity);
                        return columnMetadata;
                    }).collect(Collectors.toList());
                default:
                    break;
            }
        }
        return Collections.emptyList();
    }

    @Override
    public BrokerReference getBrokerReference() {
        MockBrokerInstance mockBrokerInstance = mockBrokerInstanceService.findBySourceId(sourceId);
        if (mockBrokerInstance != null) {
            BrokerReference brokerReference = new BrokerReference();
            brokerReference.setDataStreamId(mockBrokerInstance.getDataStreamId());
            brokerReference.setSelectedFields(mockBrokerInstance.getSelectedFields());
            brokerReference.setActive(mockBrokerInstance.getActive());
            brokerReference.setSourceId(mockBrokerInstance.getSourceId());
            brokerReference.setDocumentType(mockBrokerInstance.getDocumentType());
            brokerReference.setConnectionType(InboundConnectionType.Mock);
            return brokerReference;
        } else {
            throw new RuntimeException(String.format("Can'd find mock instance by id %s.", sourceId));
        }
    }
}
