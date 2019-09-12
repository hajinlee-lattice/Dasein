package com.latticeengines.datacloud.match.actors.visitor.impl;

import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.newSeedFromAttrs;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromAccountIdEmail;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromAccountIdNamePhoneNumber;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromCustomerAccountIdEmail;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromCustomerAccountIdNamePhoneNumber;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromEmail;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromMatchKeyTuple;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromNamePhoneNumber;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromSystemId;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.toMatchKeyTuple;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment.STAGING;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.City;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Country;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerAccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Domain;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Name;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.State;
import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.actors.ActorTemplate;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.service.EntityLookupEntryService;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import com.latticeengines.datacloud.match.service.EntityMatchVersionService;
import com.latticeengines.datacloud.match.service.EntityRawSeedService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityAssociationRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityAssociationResponse;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.testframework.service.impl.SimpleRetryAnalyzer;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import akka.util.Timeout;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

@Listeners({ SimpleRetryListener.class })
public class EntityAssociateActorTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final String TEST_ENTITY = BusinessEntity.Contact.name();
    // TODO change to InterfaceName
    private static final String CUSTOMER_CONTACT_ID = "CustomerContactId";
    private static final EntityRawSeed SEED_1 = getSeed1();
    private static final EntityRawSeed SEED_2 = getSeed2();
    private static final Map<EntityLookupEntry, String> LOOKUP_MAP = getLookupMapping(SEED_1, SEED_2);

    @Inject
    private MatchActorSystem matchActorSystem;

    @Inject
    private EntityRawSeedService entityRawSeedService;

    @Inject
    private EntityLookupEntryService entityLookupEntryService;

    @Inject
    private EntityMatchConfigurationService entityMatchConfigurationService;

    @Inject
    private EntityMatchVersionService entityMatchVersionService;

    @BeforeClass(groups = "functional")
    private void setup() {
        entityMatchConfigurationService.setIsAllocateMode(true);
        matchActorSystem.setBatchMode(true);
    }

    /**
     * Test contact match in {@link EntityAssociateActor}.
     *
     * @param tuples
     *            list of lookup match key combination (sorted by priority)
     * @param expectedEntityId
     *            expected contact entity ID, use {@literal null} to represent new
     *            contact
     * @param hasAssociationError
     *            whether there are conflict during association
     */
    @Test(groups = "functional", dataProvider = "contactMatch", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testContactAssociation(MatchKeyTuple[] tuples, String expectedEntityId, boolean hasAssociationError)
            throws Exception {
        Tenant tenant = newTestTenant();
        String entity = TEST_ENTITY;
        setupUniverse(tenant);
        boolean shouldAllocateNewEntity = expectedEntityId == null; // use null to represent new entity for now

        // prepare request
        DataSourceLookupRequest msg = getBaseRequest();
        List<Pair<MatchKeyTuple, String>> lookupResults = Arrays.stream(tuples) //
                .flatMap(tuple -> fromMatchKeyTuple(entity, tuple) //
                        .stream() //
                        .map(entry -> Pair.of(tuple, LOOKUP_MAP.get(entry)))) //
                .collect(Collectors.toList());
        EntityAssociationRequest request = new EntityAssociationRequest(tenant, entity, null, lookupResults,
                Collections.emptyMap());
        msg.setInputData(request);

        // send msg to actor
        Response response = sendMessageToActor(msg, EntityAssociateActor.class);
        Assert.assertNotNull(response);

        // verification
        EntityAssociationResponse associationResponse = (EntityAssociationResponse) response.getResult();
        Assert.assertNotNull(associationResponse);
        Assert.assertEquals(associationResponse.getEntity(), entity);
        Assert.assertEquals(associationResponse.isNewlyAllocated(), shouldAllocateNewEntity);
        if (expectedEntityId != null) {
            // associate to existing entity
            Assert.assertEquals(associationResponse.getAssociatedEntityId(), expectedEntityId);
        }

        if (hasAssociationError) {
            Assert.assertTrue(CollectionUtils.isNotEmpty(associationResponse.getAssociationErrors()),
                    "Should have association error");
        } else {
            Assert.assertFalse(CollectionUtils.isNotEmpty(associationResponse.getAssociationErrors()), String
                    .format("Should not have association error, got=%s", associationResponse.getAssociationErrors()));
        }
    }

    /*
     * Update the target seed with input attributes
     */
    @Test(groups = "functional", dataProvider = "contactMatchAttribute", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testUpdateAttributes(EntityRawSeed currSeed, String[] attributes) throws Exception {
        Tenant tenant = newTestTenant();
        String entityId = currSeed.getId();
        String entity = TEST_ENTITY;
        setupUniverse(tenant);

        EntityRawSeed seedToUpdate = newSeedFromAttrs(entityId, entity, attributes);
        Map<String, String> attrsToUpdate = seedToUpdate.getAttributes();
        DataSourceLookupRequest msg = getBaseRequest();
        // use the first lookup entry to make sure it is associated to the target
        EntityAssociationRequest request = new EntityAssociationRequest(tenant, entity, null,
                singletonList(Pair.of(toMatchKeyTuple(currSeed.getLookupEntries().get(0)), entityId)), attrsToUpdate);
        msg.setInputData(request);

        // send msg to actor
        Response response = sendMessageToActor(msg, EntityAssociateActor.class);
        Assert.assertNotNull(response);

        // verification
        EntityAssociationResponse associationResponse = (EntityAssociationResponse) response.getResult();
        Assert.assertNotNull(associationResponse);
        Assert.assertEquals(associationResponse.getAssociatedEntityId(), currSeed.getId());
        Assert.assertFalse(associationResponse.isNewlyAllocated(), "Should match to existing contact");
        Assert.assertTrue(CollectionUtils.isEmpty(associationResponse.getAssociationErrors()),
                String.format("Should not have association error, got=%s", associationResponse.getAssociationErrors()));

        // get seed object to check attributes
        Map<String, String> finalAttributes = TestEntityMatchUtils.getUpdatedAttributes(currSeed, seedToUpdate);
        EntityRawSeed seed = entityRawSeedService.get(STAGING, tenant, entity, entityId,
                entityMatchVersionService.getCurrentVersion(STAGING, tenant));
        Assert.assertNotNull(seed,
                "Updated seed(ID=" + entityId + ",entity=" + entity + ") should exist in tenant=" + tenant.getId());
        Assert.assertEquals(seed.getAttributes(), finalAttributes, "Updated attributes do not match the expected ones");
    }

    @DataProvider(name = "contactMatch")
    private Object[][] contactMatchTestData() {
        // NOTE the order of MatchKeyTuple represents the priority
        return new Object[][] { //
                // match to seed2 by email, no conflict
                { new MatchKeyTuple[] { email("h.finch@fb.com") }, SEED_2.getId(), false }, //
                // match to seed2 by email => conflict in contact ID => allocate new contact =>
                // conflict in email (already mapped to seed2)
                { new MatchKeyTuple[] { customerContactId("c3"), email("h.finch@fb.com") }, null, true }, //
                // match to seed2 by contact ID => conflict in email (already mapped to seed1)
                { new MatchKeyTuple[] { customerContactId("c2"), email("j.reese@google.com") }, SEED_2.getId(), true }, //
                { new MatchKeyTuple[] { customerContactId("c3"), acctIdEmail("a3", "h.finch@poi.com"),
                        email("h.finch@fb.com") }, null, true }, //
                // match to seed2 by contact ID => no conflict
                { new MatchKeyTuple[] { customerContactId("c2"), acctIdEmail("a3", "h.finch@poi.com") }, SEED_2.getId(),
                        false }, //
                { new MatchKeyTuple[] { customerContactId("c2"), acctIdEmail("a2", "h.finch@poi.com"),
                        namePhone("Harold Finch", "888-888-8888") }, SEED_2.getId(), false }, //
                // new account ID + email & name + phone number => allocate new contact
                // note that there is no email only tuple here to simulate that the actor is
                // skipped in one branch
                { new MatchKeyTuple[] { acctIdEmail("a3", "j.reese@google.com"),
                        acctIdNamePhone("a3", "John Reese", "000-000-0123") }, null, false }, //
        };
    }

    @DataProvider(name = "contactMatchAttribute")
    private Object[][] contactMatchAttributeTestData() {
        return new Object[][] { //
                // update name, clear domain
                { SEED_2, new String[] { Name.name(), "Netflix", Domain.name(), null } },
                // update name/domain, add country
                { SEED_2,
                        new String[] { Name.name(), "Netflix", Domain.name(), "netflix.com", Country.name(), "USA" } },
                // clear name/domain, add country
                { SEED_2, new String[] { Name.name(), null, Domain.name(), null, Country.name(), "USA" } },
                // no change
                { SEED_1, new String[] { Name.name(), "Google", Country.name(), "USA" } },
                // no change in name, update account ID, clear customer account ID
                { SEED_1,
                        new String[] { Name.name(), "Google", AccountId.name(), "a3", CustomerAccountId.name(),
                                null } },
                // add domain/city
                { SEED_1, new String[] { Domain.name(), "google.com", City.name(), "Mountain View" } }, };
    }

    private void setupUniverse(@NotNull Tenant tenant) {
        entityRawSeedService.setIfNotExists(STAGING, tenant, SEED_1, true,
                entityMatchVersionService.getCurrentVersion(STAGING, tenant));
        entityRawSeedService.setIfNotExists(STAGING, tenant, SEED_2, true,
                entityMatchVersionService.getCurrentVersion(STAGING, tenant));
        List<Pair<EntityLookupEntry, String>> lookupList = LOOKUP_MAP.entrySet().stream()
                .map(entry -> Pair.of(entry.getKey(), entry.getValue())).collect(Collectors.toList());
        entityLookupEntryService.set(STAGING, tenant, lookupList, true,
                entityMatchVersionService.getCurrentVersion(STAGING, tenant));
    }

    /*
     * send message to target actor and wait for response
     */
    private Response sendMessageToActor(Object msg, Class<? extends ActorTemplate> actorClazz) throws Exception {
        LogManager.getLogger("com.latticeengines.datacloud.match.actors.visitor").setLevel(Level.DEBUG);
        LogManager.getLogger("com.latticeengines.actors.visitor").setLevel(Level.DEBUG);

        ActorRef actorRef = matchActorSystem.getActorRef(actorClazz);

        Timeout timeout = new Timeout(new FiniteDuration(30, TimeUnit.MINUTES));
        Future<Object> future = Patterns.ask(actorRef, msg, timeout);

        return (Response) Await.result(future, timeout.duration());
    }

    private DataSourceLookupRequest getBaseRequest() {
        DataSourceLookupRequest request = new DataSourceLookupRequest();
        request.setCallerMicroEngineReference(null);
        MatchTraveler context = new MatchTraveler(UUID.randomUUID().toString(), new MatchKeyTuple());
        request.setMatchTravelerContext(context);
        return request;
    }

    private MatchKeyTuple customerContactId(String id) {
        return new MatchKeyTuple.Builder().withSystemIds(singletonList(Pair.of(CUSTOMER_CONTACT_ID, id))).build();
    }

    private MatchKeyTuple email(String email) {
        return new MatchKeyTuple.Builder().withEmail(email).build();
    }

    private MatchKeyTuple namePhone(String name, String phoneNumber) {
        return new MatchKeyTuple.Builder().withName(name).withPhoneNumber(phoneNumber).build();
    }

    private MatchKeyTuple acctIdEmail(String accountEntityId, String email) {
        return new MatchKeyTuple.Builder() //
                .withSystemIds(singletonList(Pair.of(AccountId.name(), accountEntityId))) //
                .withEmail(email) //
                .build();
    }

    private MatchKeyTuple acctIdNamePhone(String accountEntityId, String name, String phoneNumber) {
        return new MatchKeyTuple.Builder() //
                .withSystemIds(singletonList(Pair.of(AccountId.name(), accountEntityId))) //
                .withName(name) //
                .withPhoneNumber(phoneNumber) //
                .build();
    }

    private Tenant newTestTenant() {
        String tenantId = EntityAssociateActorTestNG.class.getSimpleName() + UUID.randomUUID().toString();
        return new Tenant(tenantId);
    }

    private static EntityRawSeed getSeed1() {
        // seed with account entity ID and no contact ID
        String entity = TEST_ENTITY;
        List<EntityLookupEntry> entries = new ArrayList<>();
        entries.add(fromAccountIdEmail(entity, "a1", "j.reese@google.com"));
        entries.add(fromEmail(entity, "j.reese@google.com"));
        entries.add(fromAccountIdNamePhoneNumber(entity, "a1", "John Reese", "000-000-0123"));

        Map<String, String> attributes = new HashMap<>();
        attributes.put(AccountId.name(), "a1");
        attributes.put(CustomerAccountId.name(), "ca1");
        attributes.put(Name.name(), "Google");
        attributes.put(Country.name(), "USA");
        attributes.put(State.name(), "CA");

        return new EntityRawSeed("contact-1", entity, entries, attributes);
    }

    private static EntityRawSeed getSeed2() {
        // seed with contact ID and customer account ID
        String entity = TEST_ENTITY;
        List<EntityLookupEntry> entries = new ArrayList<>();
        entries.add(fromSystemId(entity, CUSTOMER_CONTACT_ID, "c2"));
        entries.add(fromCustomerAccountIdEmail(entity, "ca2", "h.finch@fb.com"));
        entries.add(fromCustomerAccountIdNamePhoneNumber(entity, "ca2", "Harold Finch", "999-999-9876"));
        entries.add(fromEmail(entity, "h.finch@fb.com"));
        entries.add(fromNamePhoneNumber(entity, "Harold Finch", "999-999-9876"));

        Map<String, String> attributes = new HashMap<>();
        attributes.put(AccountId.name(), "a2");
        attributes.put(CustomerAccountId.name(), "ca2");
        attributes.put(Name.name(), "Facebook");
        attributes.put(Domain.name(), "fb.com");

        return new EntityRawSeed("contact-2", entity, entries, attributes);
    }

    private static Map<EntityLookupEntry, String> getLookupMapping(EntityRawSeed... seeds) {
        return Arrays.stream(seeds) //
                .filter(Objects::nonNull) //
                .flatMap(seed -> seed.getLookupEntries() //
                        .stream() //
                        .map(entry -> Pair.of(entry, seed.getId()))) //
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue, (v1, v2) -> v1));
    }
}
