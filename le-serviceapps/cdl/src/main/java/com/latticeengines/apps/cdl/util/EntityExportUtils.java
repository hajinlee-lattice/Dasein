package com.latticeengines.apps.cdl.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class EntityExportUtils {

    public static void checkExportAttribute(AtlasExportType exportType, String customerSpace,
                                            DataCollection.Version version, ServingStoreService servingStoreService) {
        List<ColumnSelection.Predefined> groups = Collections.singletonList(ColumnSelection.Predefined.Enrichment);
        List<ColumnMetadata> cms = new ArrayList<>();
        if (AtlasExportType.ACCOUNT_AND_CONTACT.equals(exportType)) {
            cms = servingStoreService.getDecoratedMetadata(customerSpace,
                    ImmutableList.copyOf(BusinessEntity.EXPORT_ENTITIES), version, groups);
        } else if (AtlasExportType.ACCOUNT.equals(exportType)) {
            List<BusinessEntity> businessEntities = BusinessEntity.EXPORT_ENTITIES.stream().collect(Collectors.toList());
            businessEntities.remove(BusinessEntity.Contact);
            cms = servingStoreService.getDecoratedMetadata(customerSpace, businessEntities, version, groups);
        } else if (AtlasExportType.CONTACT.equals(exportType)) {
            List<BusinessEntity> businessEntities = new ArrayList<>();
            businessEntities.add(BusinessEntity.Contact);
            cms = servingStoreService.getDecoratedMetadata(customerSpace, businessEntities, version, groups);
        }
        if (cms.size() == 0) {
            throw new LedpException(LedpCode.LEDP_18231, new String[]{exportType.name()});
        }
    }
}
