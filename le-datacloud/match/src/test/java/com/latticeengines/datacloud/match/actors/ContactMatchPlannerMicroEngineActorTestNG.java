package com.latticeengines.datacloud.match.actors;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.actors.visitor.impl.ContactMatchPlannerMicroEngineActor;
import com.latticeengines.domain.exposed.datacloud.match.Contact;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchKeyRecord;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput.EntityKeyMap;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ContactMatchPlannerMicroEngineActorTestNG extends SingleActorTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ContactMatchPlannerMicroEngineActorTestNG.class);

    @Test(groups = { "functional" }, dataProvider = "contactPlannerData")
    public void test(String ccid, Contact contact, String country, String parsedCcid, Contact parsedContact)
            throws Exception {
        runAndVerify(prepareMatchTraveler(ccid, contact, country, true), parsedCcid, parsedContact);
        if ("USA".equals(country)) {
            runAndVerify(prepareMatchTraveler(ccid, contact, country, false), parsedCcid, parsedContact);
        }
    }

    private void runAndVerify(MatchTraveler traveler, String parsedCcid, Contact parsedContact) throws Exception {
        traveler = (MatchTraveler) sendMessageToActor(traveler, ContactMatchPlannerMicroEngineActor.class, false);
        MatchKeyTuple tuple = traveler.getMatchKeyTuple();
        Assert.assertNotNull(tuple);
        Assert.assertEquals(tuple.getSystemIds().get(0).getRight(), parsedCcid);
        Assert.assertEquals(tuple.getName(), parsedContact.getName());
        Assert.assertEquals(tuple.getEmail(), parsedContact.getEmail());
        Assert.assertEquals(tuple.getPhoneNumber(), parsedContact.getPhoneNumber());
    }

    // Just to make sure every field in Contact is standardized. Standardization
    // correctness is verified in each util's unit test
    // CustomerContactId, RawContact, Country,
    // ParsedCustomerContactId, ParsedContact
    @DataProvider(name = "contactPlannerData")
    private Object[][] prepareContactPlannerData() {
        return new Object[][] { //
                { "CCID", new Contact("Name", "email@gmail.com", "16501234567"), "USA", //
                        "ccid", new Contact("NAME", "email@gmail.com", "16501234567") }, //
                { " CCID ", new Contact(" Name ", "gmail.com", "1 (650)123-4567"), null, //
                        "ccid", new Contact("NAME", null, "16501234567") }, //
                { null, new Contact(null, null, "134-861-9249"), "China", //
                        null, new Contact(null, null, "861348619249") }, //

        };
    }

    private MatchTraveler prepareMatchTraveler(String ccid, Contact contact, String country, boolean hasCountryKey) {
        MatchTraveler traveler = new MatchTraveler(UUID.randomUUID().toString(), null);
        // Set faked decision graph to avoid failure in GuideBook.logVisit
        traveler.setDecisionGraph("FakeDecisionGraph");
        traveler.setInputDataRecord(
                Arrays.asList(ccid, contact.getName(), contact.getEmail(), contact.getPhoneNumber(), country));
        @SuppressWarnings("serial")
        Map<MatchKey, List<Integer>> keyPositionMap = new HashMap<MatchKey, List<Integer>>() {
            {
                put(MatchKey.SystemId, Arrays.asList(0));
                put(MatchKey.Name, Arrays.asList(1));
                put(MatchKey.Email, Arrays.asList(2));
                put(MatchKey.PhoneNumber, Arrays.asList(3));
                if (hasCountryKey) {
                    put(MatchKey.Country, Arrays.asList(4));
                }
            }
        };
        @SuppressWarnings("serial")
        Map<String, Map<MatchKey, List<Integer>>> entityKeyPositionMaps = new HashMap<String, Map<MatchKey, List<Integer>>>() {
            {
                put(BusinessEntity.Contact.name(), keyPositionMap);
            }
        };
        traveler.setEntityKeyPositionMaps(entityKeyPositionMaps);
        traveler.setEntityMatchKeyRecord(new EntityMatchKeyRecord());
        MatchInput input = new MatchInput();
        input.setLogLevelEnum(Level.DEBUG);
        input.setOperationalMode(OperationalMode.ENTITY_MATCH);
        Map<String, EntityKeyMap> entityKeyMaps = new HashMap<>();
        EntityKeyMap entityKeyMap = new EntityKeyMap();
        entityKeyMap.addMatchKey(MatchKey.SystemId, InterfaceName.CustomerContactId.name());
        entityKeyMaps.put(BusinessEntity.Contact.name(), entityKeyMap);
        input.setEntityKeyMaps(entityKeyMaps);
        traveler.setMatchInput(input);
        traveler.setEntity(BusinessEntity.Contact.name());
        traveler.setReturnSender(true);
        return traveler;
    }
}
