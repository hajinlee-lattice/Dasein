package com.latticeengines.datacloud.match.service.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.log4j.Level;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.datacloud.match.testframework.TestMatchInputUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class PrimeMatchTestNG extends DataCloudMatchFunctionalTestNGBase {

    @Value("${datacloud.match.default.decision.graph.prime}")
    private String primeMatchDG;

    @Inject
    private RealTimeMatchService realTimeMatchService;

    @Test(groups = "functional")
    public void testPrimeMatch() {
        Object[][] data = new Object[][] {
                { 123, null, "Chevron Corporation", "San Ramon", "California", "USA" },
                { 456, null, "TestPublicDomainNoState", null, null, "USA" } };
        MatchInput input = matchInput(data);
        MatchOutput output = realTimeMatchService.match(input);
        System.out.println(JsonUtils.serialize(output));
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);
        for (OutputRecord outputRecord: output.getResult()) {
            if (outputRecord.isMatched()) {
                Assert.assertTrue(CollectionUtils.isNotEmpty(outputRecord.getCandidateOutput()));
            }
        }
    }

    private MatchInput matchInput(Object[][] data) {
        MatchInput matchInput = TestMatchInputUtils.prepareSimpleMatchInput(data, new String[]{
                "InternalId", "Duns", "CompanyName", "City", "Sate", "Country"
        });
        matchInput.setLogLevelEnum(Level.DEBUG);
        matchInput.setDecisionGraph(primeMatchDG);
        matchInput.setDataCloudVersion(currentDataCloudVersion);
        matchInput.setRootOperationUid(UUID.randomUUID().toString());
        matchInput.setUseDnBCache(false);
        matchInput.setUseRemoteDnB(true);
        matchInput.setAllocateId(false);
        matchInput.setEntityKeyMaps(prepareEntityKeyMap());
        matchInput.setTargetEntity(BusinessEntity.PrimeAccount.name());
        matchInput.setOperationalMode(OperationalMode.PRIME_MATCH);
        matchInput.setPredefinedSelection(null);
        matchInput.setCustomSelection(getColumnSelection());
        return matchInput;
    }

    private Map<String, MatchInput.EntityKeyMap> prepareEntityKeyMap() {
        Map<String, MatchInput.EntityKeyMap> entityKeyMaps = new HashMap<>();
        // Both Account & Contact match needs Account key map
        MatchInput.EntityKeyMap entityKeyMap = new MatchInput.EntityKeyMap();
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.DUNS, Collections.singletonList("DUNS"));
        keyMap.put(MatchKey.Name, Collections.singletonList("CompanyName"));
        keyMap.put(MatchKey.State, Collections.singletonList("State"));
        keyMap.put(MatchKey.Country, Collections.singletonList("Country"));
        entityKeyMap.setKeyMap(keyMap);
        entityKeyMaps.put(BusinessEntity.Account.name(), entityKeyMap);
        return entityKeyMaps;
    }

    private ColumnSelection getColumnSelection() {
        List<Column> columns = Stream.of(
                DataCloudConstants.ATTR_LDC_DUNS,
                DataCloudConstants.ATTR_LDC_NAME,
                "TRADESTYLE_NAME",
                "LDC_Street",
                "STREET_ADDRESS_2",
                DataCloudConstants.ATTR_CITY,
                DataCloudConstants.ATTR_STATE,
                DataCloudConstants.ATTR_ZIPCODE,
                DataCloudConstants.ATTR_COUNTRY,
                "TELEPHONE_NUMBER",
                "LE_SIC_CODE"
        ).map(Column::new).collect(Collectors.toList());
        ColumnSelection cs = new ColumnSelection();
        cs.setColumns(columns);
        return cs;
    }

}
