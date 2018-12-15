package com.latticeengines.apps.cdl.mds.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.mds.CuratedAttrsMetadataStore;
import com.latticeengines.apps.cdl.mds.TableRoleTemplate;
import com.latticeengines.apps.cdl.service.CDLNamespaceService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

@Component
public class CuratedAttrsMetadataStoreImpl implements CuratedAttrsMetadataStore {

    @Inject
    private TableRoleTemplate tableRoleTemplate;

    @Inject
    private CDLNamespaceService cdlNamespaceService;

    @Override
    public Flux<ColumnMetadata> getMetadata(Namespace2<String, DataCollection.Version> namespace) {
        Flux<ColumnMetadata> cms = Flux.empty();
        String tenantId = CustomerSpace.shortenCustomerSpace(namespace.getCoord1());
        if (StringUtils.isNotBlank(tenantId)) {
            cms = getMetadataInParallel(namespace).sequential();
        }
        return cms;
    }

    @Override
    public ParallelFlux<ColumnMetadata> getMetadataInParallel(Namespace2<String, DataCollection.Version> namespace) {
        ParallelFlux<ColumnMetadata> cms;
        String tenantId = CustomerSpace.shortenCustomerSpace(namespace.getCoord1());
        if (StringUtils.isNotBlank(tenantId)) {
            cdlNamespaceService.setMultiTenantContext(tenantId);
            DataCollection.Version version = namespace.getCoord2();
            TableRoleInCollection role = TableRoleInCollection.CalculatedCuratedAccountAttribute;
            Namespace2<TableRoleInCollection, DataCollection.Version> trNs = Namespace.as(role, version);
            ParallelFlux<ColumnMetadata> servingStore = tableRoleTemplate.getUnorderedSchema(trNs);
            cms = servingStore.map(cm -> {
                cm.setAttrState(AttrState.Active);

                cm.disableGroup(ColumnSelection.Predefined.Segment);
                cm.disableGroup(ColumnSelection.Predefined.Enrichment);
                cm.enableGroup(ColumnSelection.Predefined.TalkingPoint);
                cm.disableGroup(ColumnSelection.Predefined.CompanyProfile);
                cm.disableGroup(ColumnSelection.Predefined.Model);

                cm.setCategory(Category.CURATED_ACCOUNT_ATTRIBUTES);
                return cm;
            });
        } else {
            cms = Flux.<ColumnMetadata>empty().parallel().runOn(scheduler);
        }
        return cms;
    }

}
