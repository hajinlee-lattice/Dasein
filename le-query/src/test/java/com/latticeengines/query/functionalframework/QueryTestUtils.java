package com.latticeengines.query.functionalframework;

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

    private static AttributeRepository amAttrRepo;

    public static AttributeRepository getCustomerAttributeRepo() {
        CustomerSpace customerSpace = CustomerSpace.parse("Query");
        String collectionName = "querytest";
        Map<AttributeLookup, ColumnMetadata> attrMap = getAttrMap();
        Map<TableRoleInCollection, String> tableNameMap = new HashMap<>();
        tableNameMap.put(BucketedAccount, "querytest_table");
        tableNameMap.put(BucketedContact, "querytest_table_dup");
        tableNameMap.put(AccountMaster, "querytest_table_dup");
        return new AttributeRepository(customerSpace, collectionName, attrMap, tableNameMap);
    }

    public static AttributeRepository getAMAttributeRepo() {
        if (amAttrRepo == null) {
            CustomerSpace customerSpace = CustomerSpace.parse("Query");
            String collectionName = "querytest";
            Map<AttributeLookup, ColumnMetadata> attrMap = getAttrMap();
            Map<TableRoleInCollection, String> tableNameMap = new HashMap<>();
            tableNameMap.put(AccountMaster, "querytest_table_dup");
            amAttrRepo = new AttributeRepository(customerSpace, collectionName, attrMap, tableNameMap);
        }
        return amAttrRepo;
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
                "AccountId", "LatticeAccountId", "SalesForceAccountId", "LastModified", "CompanyName", "ID", "City",
                "State", "LastName", "Number_Of_Family_Members", "AlexaViewsPerUser");
        List<ColumnMetadata> attrInTable = new ArrayList<>();
        simpleFields.forEach(s -> attrInTable.add(new Attribute(s).getColumnMetadata()));
        Attribute bucketedAttribute = new Attribute("Bucketed_Attribute");
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
        bucketedAttribute.setPhysicalName("BusinessTechnologiesPayment");
        bucketedAttribute.setBitOffset(1);
        bucketedAttribute.setNumOfBits(2);
        ColumnMetadata bucketCm = bucketedAttribute.getColumnMetadata();
        bucketCm.setStats(stats);
        attrInTable.add(bucketCm);
        return attrInTable;
    }

}
