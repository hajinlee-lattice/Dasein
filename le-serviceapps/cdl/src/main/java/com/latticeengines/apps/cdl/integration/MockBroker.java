package com.latticeengines.apps.cdl.integration;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.latticeengines.apps.cdl.service.MockBrokerInstanceService;
import com.latticeengines.domain.exposed.cdl.IngestionScheduler;
import com.latticeengines.domain.exposed.cdl.MockBrokerInstance;
import com.latticeengines.domain.exposed.cdl.integration.BrokerReference;
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
            InterfaceName.City.name(), InterfaceName.Country.name(), InterfaceName.DUNS.name(), InterfaceName.PhoneNumber.name(),
            InterfaceName.Website.name(), InterfaceName.Industry.name());

    private final List<String> contactAttributes = Lists.newArrayList(InterfaceName.AccountId.name(), InterfaceName.ContactId.name(), InterfaceName.Email.name(),
            InterfaceName.FirstName.name(), InterfaceName.LastName.name(), InterfaceName.Title.name());

    @Inject
    private MockBrokerInstanceService mockBrokerInstanceService;

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
            mockBrokerInstance.setIngestionScheduler(scheduler);
            mockBrokerInstanceService.createOrUpdate(mockBrokerInstance);
        } else {
            log.info(String.format("Can'd find mock instance by id %s.", sourceId));
        }
    }

    @Override
    public void start() {
        setActive(true);
    }

    private void setActive(boolean active) {
        MockBrokerInstance mockBrokerInstance = mockBrokerInstanceService.findBySourceId(sourceId);
        if (mockBrokerInstance != null) {
            mockBrokerInstance.setActive(active);
            mockBrokerInstanceService.createOrUpdate(mockBrokerInstance);
        } else {
            log.info(String.format("Can'd find mock instance by id %s.", sourceId));
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

}
