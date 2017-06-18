package com.latticeengines.query.evaluator;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AccountMaster;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.BucketedAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.BucketedContact;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.BucketType;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class QueryTestUtils {

    static AttributeRepository getAttributeRepo() {
        CustomerSpace customerSpace = CustomerSpace.parse("Query");
        String collectionName = "querytest";
        Map<AttributeLookup, Attribute> attrMap = getAttrMap();
        Map<TableRoleInCollection, String> tableNameMap = getTableNameMap();
        return new AttributeRepository(customerSpace, collectionName, attrMap, tableNameMap);
    }

    private static Map<TableRoleInCollection, String> getTableNameMap() {
        Map<TableRoleInCollection, String> map = new HashMap<>();
        map.put(BucketedAccount, "querytest_table");
        map.put(AccountMaster, "querytest_table_dup");
        map.put(BucketedContact, "querytest_table_dup");
        return map;
    }

    private static Map<AttributeLookup, Attribute> getAttrMap() {
        Map<AttributeLookup, Attribute> attrMap = new HashMap<>();
        for (BusinessEntity entity : Arrays.asList(BusinessEntity.Account, BusinessEntity.LatticeAccount,
                BusinessEntity.Contact)) {
            getAttrsInTable().forEach(attr -> {
                AttributeLookup lookup = new AttributeLookup(entity, attr.getName());
                attrMap.put(lookup, attr);
            });
        }
        return attrMap;
    }

    private static List<Attribute> getAttrsInTable() {
        List<String> simpleFields = Arrays.asList( //
                "companyname", "id", "city", "state", "lastname", "number_of_family_members", "alexaviewsperuser");
        List<Attribute> attrInTable = new ArrayList<>();
        simpleFields.forEach(s -> attrInTable.add(new Attribute(s)));
        Attribute bucketedAttribute = new Attribute("bucketed_attribute");

        AttributeStats stats = new AttributeStats();
        Buckets bkts = new Buckets();
        bkts.setType(BucketType.Enum);
        Bucket bkt1 = new Bucket();
        bkt1.setBucketLabel("Label1");
        bkt1.setId(0L);
        Bucket bkt2 = new Bucket();
        bkt2.setBucketLabel("Label2");
        bkt2.setId(1L);
        Bucket bkt3 = new Bucket();
        bkt3.setBucketLabel("Label3");
        bkt3.setId(2L);
        bkts.setBucketList(Arrays.asList(bkt1, bkt2, bkt3));
        stats.setBuckets(bkts);
        bucketedAttribute.setStats(stats);
        bucketedAttribute.setPhysicalName("businesstechnologiespayment");
        bucketedAttribute.setBitOffset(1);
        bucketedAttribute.setNumOfBits(2);
        attrInTable.add(bucketedAttribute);
        return attrInTable;
    }

}
