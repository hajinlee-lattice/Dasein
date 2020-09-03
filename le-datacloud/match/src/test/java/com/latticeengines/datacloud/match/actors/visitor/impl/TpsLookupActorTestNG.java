package com.latticeengines.datacloud.match.actors.visitor.impl;

import static com.latticeengines.datacloud.match.domain.TpsLookupResult.ReturnCode.EmptyResult;
import static com.latticeengines.datacloud.match.domain.TpsLookupResult.ReturnCode.Ok;
import static com.latticeengines.datacloud.match.domain.TpsLookupResult.ReturnCode.UnknownLocalError;
import static com.latticeengines.datacloud.match.domain.TpsLookupResult.ReturnCode.UnknownRemoteError;

import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.match.actors.SingleActorTestNGBase;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.domain.TpsLookupResult;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

@Component
public class TpsLookupActorTestNG extends SingleActorTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(TpsLookupActorTestNG.class);

    private static final String REMOTE_ERROR_DUNS = "121";
    private static final String LOCAL_ERROR_DUNS = "122";

    @Inject
    private TpsLookupService tpsLookupService;

    @BeforeClass(groups = "functional")
    public void setup() {
        tpsLookupService.addMockError(REMOTE_ERROR_DUNS, UnknownRemoteError);
        tpsLookupService.addMockError(LOCAL_ERROR_DUNS, UnknownLocalError);
    }

    @Test(groups = "functional", dataProvider = "tpsLookupActorData", priority = 1)
    public void testTpsActorRealtimeMode(String duns, TpsLookupResult.ReturnCode expectedError) throws Exception {
        testTpsActor(duns, false, expectedError);
    }

    @Test(groups = "functional", dataProvider = "tpsLookupActorData", priority = 2)
    public void testTpsActorBatchMode(String duns, TpsLookupResult.ReturnCode expectedError) throws Exception {
        testTpsActor(duns, true, expectedError);
    }

    private void testTpsActor(String duns, boolean isBatchMode, TpsLookupResult.ReturnCode expectedError)
            throws Exception {
        actorSystem.setBatchMode(isBatchMode);
        DataSourceLookupRequest msg = new DataSourceLookupRequest();
        msg.setCallerMicroEngineReference(null);
        MatchKeyTuple.Builder tupleBuilder = new MatchKeyTuple.Builder();
        MatchKeyTuple matchKeyTuple = tupleBuilder //
                .withDuns(duns) //
                .build();
        msg.setInputData(matchKeyTuple);
        String rootOperationUid = UUID.randomUUID().toString();
        MatchTraveler matchTravelerContext = new MatchTraveler(rootOperationUid, matchKeyTuple);

        MatchInput matchInput = new MatchInput();
        matchInput.setUseRemoteDnB(Boolean.TRUE);
        matchInput.setDataCloudVersion(currentDataCloudVersion);
        matchTravelerContext.setMatchInput(matchInput);
        msg.setMatchTravelerContext(matchTravelerContext);

        Response result = (Response) sendMessageToActor(msg, TpsLookupActor.class, false);
        Assert.assertNotNull(result);
        TpsLookupResult data = (TpsLookupResult) result.getResult();
        Assert.assertNotNull(data);
        System.out.println(JsonUtils.pprint(data));
        if (expectedError != null) {
            verifyErrorResult(data, expectedError);
        } else {
            verifyOkResult(data);
        }
    }

    private void verifyErrorResult(TpsLookupResult data, TpsLookupResult.ReturnCode expectedError) {
        Assert.assertEquals(data.getReturnCode(), expectedError);
    }

    private void verifyOkResult(TpsLookupResult data) {
        if (Ok.equals(data.getReturnCode())) {
            List<String> recordIds = data.getRecordIds();
            Assert.assertFalse(recordIds.isEmpty());
            // Add few assertions for record uuids
            // Might need to update once dataset has changed
            Assert.assertTrue(recordIds.contains("23219b43-bf68-46e1-a549-3161c5e66c20"));
            Assert.assertTrue(recordIds.contains("9531eac4-b6d3-4367-864f-c7f6c7022012"));
            Assert.assertTrue(recordIds.contains("7a552ad6-ca82-4912-99b0-02bb680363f9"));
            Assert.assertTrue(recordIds.contains("03692007-c329-4138-882b-cc3e124a3074"));
        } else {
            Assert.assertTrue(CollectionUtils.isEmpty(data.getRecordIds()));
            Assert.assertEquals(data.getReturnCode(), EmptyResult);
        }
    }

    @DataProvider(name = "tpsLookupActorData")
    private Object[][] provideTpsLookupData() {
        // duns
        return new Object[][] { //
                { "028675958", null }, // Use LATTICE ENGINES site duns for testing
                { "918536814", EmptyResult}, // Non-existing duns
                { null, null }, //
                { REMOTE_ERROR_DUNS, UnknownRemoteError }, // UnknownRemoteError
                { LOCAL_ERROR_DUNS, UnknownLocalError }, // UnknownLocalError
        };
    }
}
