package com.latticeengines.cdl.workflow.steps.export;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportContactFetcher {

    private static final Logger log = LoggerFactory.getLogger(ExportContactFetcher.class);

    @Autowired
    private EntityProxy entityProxy;

    @Value("${playmaker.workflow.segment.pagesize:100}")
    private long pageSize;

    public long getCount(SegmentExportContext segmentExportContext, DataCollection.Version version) {
        log.info(String.format("Requesting count for payload: %s", //
                segmentExportContext.getContactFrontEndQuery() == null //
                        ? "null" : JsonUtils.serialize(segmentExportContext.getClonedContactFrontEndQuery())));
        return entityProxy.getCountFromObjectApi( //
                segmentExportContext.getCustomerSpace().toString(), //
                segmentExportContext.getClonedContactFrontEndQuery(), version);
    }

    public Map<Object, List<Map<String, String>>> fetch(SegmentExportContext segmentExportContext,
            DataCollection.Version version) {
        Map<Object, List<Map<String, String>>> mapForAccountAndContactList = new HashMap<>();

        try {
            FrontEndQuery contactFrontEndQuery = segmentExportContext.getClonedContactFrontEndQuery();

            contactFrontEndQuery.setPageFilter(null);
            log.info(String.format("Contact query => %s", JsonUtils.serialize(contactFrontEndQuery)));

            Long contactsCount = entityProxy.getCountFromObjectApi( //
                    segmentExportContext.getCustomerSpace().toString(), //
                    contactFrontEndQuery, version);
            int pages = (int) Math.ceil((contactsCount * 1.0D) / pageSize);

            log.info("Number of required loops for fetching contacts: " + pages + ", with pageSize: " + pageSize);
            long processedContactsCount = 0L;

            for (int pageNo = 0; pageNo < pages; pageNo++) {
                processedContactsCount = fetchContactsPage(segmentExportContext, mapForAccountAndContactList,
                        contactsCount, processedContactsCount, pageNo, version);
            }

        } catch (Exception ex) {
            log.error("Ignoring till contact data is available in cdl", ex);
        }

        return mapForAccountAndContactList;
    }

    private long fetchContactsPage(SegmentExportContext segmentExportContext,
            Map<Object, List<Map<String, String>>> mapForAccountAndContactList, Long contactsCount,
            long processedContactsCount, int pageNo, DataCollection.Version version) {
        FrontEndQuery contactFrontEndQuery = segmentExportContext.getClonedContactFrontEndQuery();

        log.info(String.format("Contacts Loop #%d", pageNo));
        long expectedPageSize = Math.min(pageSize, (contactsCount - processedContactsCount));

        contactFrontEndQuery.setPageFilter(new PageFilter(processedContactsCount, expectedPageSize));

        log.info(String.format("Contact query => %s", JsonUtils.serialize(contactFrontEndQuery)));

        DataPage contactPage = entityProxy.getDataFromObjectApi( //
                segmentExportContext.getCustomerSpace().toString(), //
                contactFrontEndQuery, version, true);

        log.info(String.format("Got # %d contact elements in this loop", contactPage.getData().size()));
        processedContactsCount += contactPage.getData().size();

        contactPage //
                .getData() //
                .forEach( //
                        contact -> processContToUpdMapForAccContList(mapForAccountAndContactList, contact));
        return processedContactsCount;
    }

    private void processContToUpdMapForAccContList(Map<Object, List<Map<String, String>>> mapForAccountAndContactList,
            Map<String, Object> contact) {
        Object accountIdObj = contact.get(InterfaceName.AccountId.name());
        if (accountIdObj != null) {
            if (!mapForAccountAndContactList.containsKey(accountIdObj)) {
                mapForAccountAndContactList.put(accountIdObj, new ArrayList<>());
            }
            List<Map<String, String>> contacts = mapForAccountAndContactList.get(accountIdObj);
            contacts.add(convertValuesToString(contact));
        }
    }

    private Map<String, String> convertValuesToString(Map<String, Object> contact) {
        Map<String, String> contactWithStringValues = new HashMap<>();
        for (String key : contact.keySet()) {
            String keyWithPrefix = BusinessEntity.Contact.name() + SegmentExportProcessor.SEPARATOR + key;
            contactWithStringValues.put(keyWithPrefix, contact.get(key) == null ? null : contact.get(key).toString());
        }

        return contactWithStringValues;
    }

    public DataPage fetch(SegmentExportContext segmentExportContext, long segmentContactsCount,
            long processedSegmentContactsCount, DataCollection.Version version) {
        long expectedPageSize = Math.min(pageSize, (segmentContactsCount - processedSegmentContactsCount));
        FrontEndQuery contactFrontEndQuery = segmentExportContext.getClonedContactFrontEndQuery();
        contactFrontEndQuery.setPageFilter(new PageFilter(processedSegmentContactsCount, expectedPageSize));

        log.info(String.format("Contact query => %s", JsonUtils.serialize(contactFrontEndQuery)));

        DataPage contactPage = entityProxy.getDataFromObjectApi( //
                segmentExportContext.getCustomerSpace().toString(), //
                contactFrontEndQuery, version, true);

        log.info(String.format("Got # %d elements in this loop", contactPage.getData().size()));
        return contactPage;
    }

    @VisibleForTesting
    void setEntityProxy(EntityProxy entityProxy) {
        this.entityProxy = entityProxy;
    }

    @VisibleForTesting
    void setPageSize(long pageSize) {
        this.pageSize = pageSize;
    }
}
