package com.latticeengines.domain.exposed.datacloud.match.entity;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerAccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerContactId;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class EntityLookupEntryUnitTestNG {

    // entity does not matter to this test for now, just choose one
    private static final String TEST_ENTITY = BusinessEntity.Contact.name();

    @Test(groups = "unit", dataProvider = "toNFromMatchKeyTuple")
    private void testToNFromMatchKeyTuple(MatchKeyTuple tuple) {
        for (EntityLookupEntry.Type type : EntityLookupEntry.Type.values()) {
            if (type.canTransformToEntries(tuple)) {
                List<EntityLookupEntry> entries = type.toEntries(TEST_ENTITY, tuple);
                Assert.assertNotNull(entries, "Transformed list of lookup entries should not be null");
                Assert.assertFalse(entries.isEmpty(), "Transformed list of lookup entries should not be empty");
                Assert.assertFalse(entries.stream().anyMatch(Objects::isNull),
                        String.format("Should not have any null lookup entry in the transformed list = %s", entries));
                List<MatchKeyTuple> tuples = entries.stream().map(type::toTuple).collect(Collectors.toList());
                Assert.assertNotNull(tuples, "List of transformed tuples should not be null");
                Assert.assertEquals(tuples.size(), entries.size(),
                        "List of transformed tuples should have the same size as entries");
                Assert.assertFalse(tuples.stream().anyMatch(Objects::isNull),
                        String.format("Should not have any null tuple in the transformed list = %s", tuples));
            } else {
                Assert.assertThrows(IllegalArgumentException.class, () -> type.toEntries(TEST_ENTITY, tuple));
            }
        }
    }

    // TODO add unit tests for toEntries and toTuple separately

    @DataProvider(name = "toNFromMatchKeyTuple")
    private Object[][] toNFromMatchKeyTupleTestData() {
        return new Object[][] { //
                // account match key combination
                { new MatchKeyTuple.Builder().withName("Google").withCountry("USA").build() }, //
                { new MatchKeyTuple.Builder().withDomain("google.com").withCountry("USA").build() }, //
                { new MatchKeyTuple.Builder().withDuns("111111111").build() }, //
                { systemId(CustomerAccountId, "ca1").build() }, //
                { systemId(AccountId, "a1").build() }, //
                // contact match key combination
                { new MatchKeyTuple.Builder().withEmail("h.finch@google.com").build() }, //
                { new MatchKeyTuple.Builder().withName("Harold Finch").withPhoneNumber("999-000-1234").build() }, //
                { systemId(CustomerContactId, "cc1").build() }, //
                { systemId(AccountId, "a1").withEmail("h.finch@google.com").build() }, //
                { systemId(CustomerAccountId, "a1").withEmail("h.finch@google.com").build() }, //
                { systemId(AccountId, "a1").withName("Harold Finch").withPhoneNumber("999-000-1234").build() }, //
                { systemId(CustomerAccountId, "a1").withName("Harold Finch").withPhoneNumber("999-000-1234").build() }, //
                // TODO add more variation on each field's value
        };
    }

    private MatchKeyTuple.Builder systemId(InterfaceName col, String id) {
        return new MatchKeyTuple.Builder().withSystemIds(ImmutableList.of(Pair.of(col.name(), id)));
    }
}
