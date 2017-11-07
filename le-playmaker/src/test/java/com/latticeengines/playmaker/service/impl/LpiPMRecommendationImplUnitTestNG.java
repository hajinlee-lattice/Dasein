package com.latticeengines.playmaker.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyListOf;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmakercore.SynchronizationDestinationEnum;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.playmakercore.entitymanager.RecommendationEntityMgr;
import com.latticeengines.playmakercore.service.impl.LpiPMRecommendationImpl;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class LpiPMRecommendationImplUnitTestNG {

    private LpiPMRecommendationImpl lpiPMRecommendationImpl;

    @Mock
    private RecommendationEntityMgr recommendationEntityMgr;

    @Mock
    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    private String playId;
    private String playLaunchId;
    private List<String> idList;
    private int TOTAL_REC_COUNT;
    private List<Map<String, Object>> resultMaps;

    @BeforeClass(groups = "unit")
    public void setup() {
        String randId = UUID.randomUUID().toString();
        String tenantIdentifier = randId + "." + randId + ".Production";
        playId = "play__" + randId;
        playLaunchId = "launch__" + randId;
        TOTAL_REC_COUNT = 2;
        idList = new ArrayList<>();
        idList.add(playId);

        resultMaps = createDummyRecommendationResult(TOTAL_REC_COUNT);

        MockitoAnnotations.initMocks(this);

        mockRecommendationEntityMgr(TOTAL_REC_COUNT);
        mockInternalResourceRestApiProxy();
        MultiTenantContext.setTenant(new Tenant("a.a.Production"));

        lpiPMRecommendationImpl = new LpiPMRecommendationImpl();

        lpiPMRecommendationImpl.setRecommendationEntityMgr(recommendationEntityMgr);
        lpiPMRecommendationImpl.setInternalResourceRestApiProxy(internalResourceRestApiProxy);
    }

    @Test(groups = "unit")
    public void testGetRecommendationCount() {
        int count = lpiPMRecommendationImpl.getRecommendationCount(0, SynchronizationDestinationEnum.SFDC, idList);
        Assert.assertEquals(TOTAL_REC_COUNT, count);
    }

    @Test(groups = "unit")
    public void testGetRecommendations() {
        List<Map<String, Object>> recommendations = lpiPMRecommendationImpl.getRecommendations(0, 0,
                TOTAL_REC_COUNT + 5, SynchronizationDestinationEnum.SFDC, idList);
        Assert.assertTrue(recommendations != null);
        Assert.assertFalse(recommendations.isEmpty());
        Assert.assertEquals(TOTAL_REC_COUNT, recommendations.size());

        int idx = 0;

        for (Map<String, Object> recommendation : recommendations) {
            Map<String, Object> expectedRecData = resultMaps.get(idx);
            Assert.assertEquals(expectedRecData.get(PlaymakerConstants.AccountID),
                    recommendation.get(PlaymakerConstants.AccountID));

            if (recommendation.containsKey(PlaymakerConstants.PlayID)) {
                Assert.assertEquals(expectedRecData.get(PlaymakerConstants.PlayID),
                        recommendation.get(PlaymakerConstants.PlayID));
            }

            Assert.assertEquals(++idx, recommendation.get(PlaymakerConstants.RowNum));

        }
        System.out.println(JsonUtils.serialize(recommendations));

    }

    @SuppressWarnings("deprecation")
    private void mockRecommendationEntityMgr(long pageSize) {
        when(recommendationEntityMgr //
                .findRecommendationCount( //
                        any(Date.class), //
                        anyString(), //
                        anyListOf(String.class))) //
                                .thenReturn(TOTAL_REC_COUNT);

        when(recommendationEntityMgr //
                .findRecommendationsAsMap( //
                        any(Date.class), //
                        anyInt(), //
                        anyInt(), //
                        anyString(), //
                        anyListOf(String.class))) //
                                .thenReturn(resultMaps);
    }

    private void mockInternalResourceRestApiProxy() {
        List<Play> plays = new ArrayList<>();
        Play play = new Play();
        play.setPid(1L);
        play.setName(playId);
        play.setDisplayName("My Play");
        play.setDescription("Play for business usecase");
        plays.add(play);
        when(internalResourceRestApiProxy //
                .getPlays(any(CustomerSpace.class))) //
                        .thenReturn(plays);
    }

    private List<Map<String, Object>> createDummyRecommendationResult(int count) {
        RuleBucketName[] buckets = new RuleBucketName[] { RuleBucketName.A, RuleBucketName.A_MINUS };
        List<Map<String, Object>> result = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            Map<String, Object> rec = new HashMap<>();

            rec.put(PlaymakerConstants.AccountID, "" + i);

            rec.put(PlaymakerConstants.PlayID, playId);

            rec.put(PlaymakerConstants.LaunchID, playLaunchId);

            rec.put(PlaymakerConstants.LaunchDate, 100L);

            rec.put(PlaymakerConstants.PriorityDisplayName, buckets[i % buckets.length].getName());

            rec.put(PlaymakerConstants.PriorityID, buckets[i % buckets.length].name());

            rec.put(PlaymakerConstants.Contacts,
                    "[{\"Email\":\"FirstName4679@ort.com\",\"Address\":\"Marine Corps Personnel Support Dr\","
                            + "\"Phone\":\"248.813.2000\",\"State\":\"MI\",\"ZipCode\":\"48098-2815\",\"Country\":\"USA\","
                            + "\"SfdcContactID\":\"\",\"City\":\"Troy\",\"ContactID\":\"4679\",\"Name\":\"FirstName4679 LastName4679\"}]");

            rec.put(InterfaceName.CompanyName.name(), "Company " + i);

            result.add(rec);
        }

        return result;
    }

}
