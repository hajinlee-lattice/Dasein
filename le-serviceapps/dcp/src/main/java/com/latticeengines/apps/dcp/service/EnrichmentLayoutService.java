package com.latticeengines.apps.dcp.service;

import java.util.Arrays;
import java.util.List;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayoutDetail;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayoutSummary;

public interface EnrichmentLayoutService {

    ResponseDocument<String> create(String customerSpace, EnrichmentLayout enrichmentLayout);

    ResponseDocument<String> update(String customerSpace, EnrichmentLayout enrichmentLayout);

    EnrichmentLayout findByLayoutId(String customerSpace, String layoutId);

    EnrichmentLayout findBySourceId(String customerSpace, String sourceId);

    EnrichmentLayoutDetail findEnrichmentLayoutDetailByLayoutId(String customerSpace, String layoutId);

    EnrichmentLayoutDetail findEnrichmentLayoutDetailBySourceId(String customerSpace, String layoutId);

    void deleteLayoutByLayoutId(String customerSpace, String layoutId);

    void deleteLayoutBySourceId(String customerSpace, String sourceId);

    void deleteLayout(EnrichmentLayout enrichmentLayout);

    List<EnrichmentLayoutSummary> getAll(String customerSpace, int pageIndex, int pageSize);

    default EnrichmentLayout getDefaultLayout() {
        EnrichmentLayout enrichmentLayout = new EnrichmentLayout();
        enrichmentLayout.setDomain(DataDomain.SalesMarketing);
        enrichmentLayout.setRecordType(DataRecordType.Domain);
        enrichmentLayout.setElements(Arrays.asList( //
                "duns_number", //
                "primaryname", //
                "countryisoalpha2code", //
                "tradestylenames_name", //
                "primaryaddr_street_line1", //
                "primaryaddr_street_line2", //
                "primaryaddr_addrlocality_name", //
                "primaryaddr_addrregion_name", //
                "primaryaddr_postalcode", //
                "primaryaddr_country_name", //
                "telephone_telephonenumber", //
                "primaryindcode_ussicv4" //
        ));
        return enrichmentLayout;
    }

}
