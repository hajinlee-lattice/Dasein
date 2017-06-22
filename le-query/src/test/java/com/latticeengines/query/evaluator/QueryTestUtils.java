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
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class QueryTestUtils {

    static AttributeRepository getAttributeRepo() {
        CustomerSpace customerSpace = CustomerSpace.parse("Query");
        String collectionName = "querytest";
        Map<AttributeLookup, ColumnMetadata> attrMap = getAttrMap();
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

    private static Map<AttributeLookup, ColumnMetadata> getAttrMap() {
        Map<AttributeLookup, ColumnMetadata> attrMap = new HashMap<>();
        for (BusinessEntity entity : Arrays.asList(BusinessEntity.Account, BusinessEntity.LatticeAccount,
                BusinessEntity.Contact)) {
            getAttrsInTable().forEach(cm -> {
                AttributeLookup lookup = new AttributeLookup(entity, cm.getName());
                attrMap.put(lookup, cm);
            });
        }
        return attrMap;
    }

    private static List<ColumnMetadata> getAttrsInTable() {
        List<String> simpleFields = Arrays.asList( //
                "companyname", "id", "city", "state", "lastname", "number_of_family_members", "alexaviewsperuser");
        List<ColumnMetadata> attrInTable = new ArrayList<>();
        simpleFields.forEach(s -> attrInTable.add(new Attribute(s).getColumnMetadata()));
        Attribute bucketedAttribute = new Attribute("bucketed_attribute");
        AttributeStats stats = new AttributeStats();
        Buckets bkts = new Buckets();
        bkts.setType(BucketType.Enum);
        Bucket bkt1 = new Bucket();
        bkt1.setLabel("Label1");
        bkt1.setId(0L);
        Bucket bkt2 = new Bucket();
        bkt2.setLabel("Label2");
        bkt2.setId(1L);
        Bucket bkt3 = new Bucket();
        bkt3.setLabel("Label3");
        bkt3.setId(2L);
        bkts.setBucketList(Arrays.asList(bkt1, bkt2, bkt3));
        stats.setBuckets(bkts);
        bucketedAttribute.setPhysicalName("businesstechnologiespayment");
        bucketedAttribute.setBitOffset(1);
        bucketedAttribute.setNumOfBits(2);
        ColumnMetadata bucketCm = bucketedAttribute.getColumnMetadata();
        bucketCm.setStats(stats);
        attrInTable.add(bucketCm);
        return attrInTable;
    }

}
