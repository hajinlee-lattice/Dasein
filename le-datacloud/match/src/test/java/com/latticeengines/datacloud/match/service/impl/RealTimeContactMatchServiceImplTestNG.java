package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.datacloud.match.MatchKey.DUNS;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.datacloud.match.testframework.TestMatchInputService;
import com.latticeengines.domain.exposed.datacloud.contactmaster.ContactMasterConstants;
import com.latticeengines.domain.exposed.datacloud.contactmaster.LiveRampDestination;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.config.TpsMatchConfig;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

@Component
public class RealTimeContactMatchServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RealTimeContactMatchServiceImplTestNG.class);

    @Inject
    private RealTimeMatchService realTimeMatchService;

    @Inject
    private TestMatchInputService testMatchInputService;

    @Test(groups = "functional")
    public void testSimpleTpsMatch() {
        Object[][] data = new Object[][] { //
                { 1, "039596507" }, //
        };
        MatchInput input = testMatchInputService.prepareSimpleRTSMatchInput(data);
        input.setFields(Arrays.asList("ID", "SiteDuns"));
        input.setKeyMap(ImmutableMap.<MatchKey, List<String>> builder().put(DUNS, Collections.singletonList("SiteDuns"))
                .build());
        input.setSkipKeyResolution(true);

        input.setTargetEntity(ContactMasterConstants.MATCH_ENTITY_TPS);
        input.setOperationalMode(OperationalMode.CONTACT_MATCH);
        input.setPredefinedSelection(ColumnSelection.Predefined.ID);

        TpsMatchConfig matchConfig = new TpsMatchConfig();
        matchConfig.setDestination(LiveRampDestination.Adobe);
        matchConfig.setJobFunctions(Arrays.asList("Administrative", "Engineering"));
        matchConfig.setJobLevels(Arrays.asList("CXO", "VP"));
        input.setTpsMatchConfig(matchConfig);

        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);
        System.out.println("Match output fields: " + output.getOutputFields());
        System.out.println("Match output: " + JsonUtils.serialize(output));
        Assert.assertEquals(output.getResult().size(), data.length);
        List<List<Object>> candidateOutputs = output.getResult().get(0).getCandidateOutput();
        Assert.assertTrue(CollectionUtils.isNotEmpty(candidateOutputs));
        Assert.assertEquals(candidateOutputs.size(), 5);
        for(List<Object> list: candidateOutputs) {
            Assert.assertTrue(Arrays.asList("Administrative", "Engineering").contains(list.get(14)));
            Assert.assertTrue(Arrays.asList("CXO", "VP").contains(list.get(13)));
        }
    }
}
