package com.latticeengines.query.functionalframework;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.zip.GZIPInputStream;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;

public class QueryTestUtils {

    private static AttributeRepository attrRepo;

    public static final String ATTR_REPO_S3_DIR = "le-query/attrrepo";
    public static final String ATTR_REPO_S3_FILENAME = "attrrepo.json.gz";
    public static final String TABLES_S3_FILENAME = "Tables.zip";
    public static final String TABLEJSONS_S3_FILENAME = "TableJsons.zip";

    public static AttributeRepository getCustomerAttributeRepo(InputStream is) {
        if (attrRepo == null) {
            try {
                GZIPInputStream gis = new GZIPInputStream(is);
                attrRepo = JsonUtils.deserialize(gis, AttributeRepository.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to read attrrepo.json.gz", e);
            }
        }
        return attrRepo;
    }

    public static Collection<TableRoleInCollection> getRolesInAttrRepo() {
        return Arrays.asList( //
                TableRoleInCollection.BucketedAccount,
                TableRoleInCollection.SortedContact,
                TableRoleInCollection.AggregatedTransaction,
                TableRoleInCollection.AggregatedPeriodTransaction,
                TableRoleInCollection.CalculatedDepivotedPurchaseHistory,
                TableRoleInCollection.CalculatedPurchaseHistory,
                TableRoleInCollection.PivotedRating,
                TableRoleInCollection.CalculatedCuratedAccountAttribute,
                TableRoleInCollection.SortedProduct
        );
    }

    public static String getServingStoreName(TableRoleInCollection role, int version) {
        return String.format("Query_Test_%s_%d", role, version);
    }
}
