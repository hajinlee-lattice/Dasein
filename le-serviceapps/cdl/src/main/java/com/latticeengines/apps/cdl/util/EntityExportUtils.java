package com.latticeengines.apps.cdl.util;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

public class EntityExportUtils {

    public static void checkExportAttribute(AtlasExportType exportType, String customerSpace,
                                            DataCollection.Version version, ServingStoreService servingStoreService) {
        List<ColumnMetadata> cms = new ArrayList<>();
        if (AtlasExportType.ACCOUNT_AND_CONTACT.equals(exportType)) {
            cms = servingStoreService.getAccountMetadata(customerSpace, ColumnSelection.Predefined.Enrichment, version);
            cms.addAll(servingStoreService.getContactMetadata(customerSpace, ColumnSelection.Predefined.Enrichment, version));
        } else if (AtlasExportType.ACCOUNT.equals(exportType)) {
            cms = servingStoreService.getAccountMetadata(customerSpace, ColumnSelection.Predefined.Enrichment, version);
        } else if (AtlasExportType.CONTACT.equals(exportType)) {
            cms = servingStoreService.getContactMetadata(customerSpace, ColumnSelection.Predefined.Enrichment, version);
        }
        if (cms.size() == 0) {
            throw new LedpException(LedpCode.LEDP_18231, new String[]{exportType.name()});
        }
    }
}
