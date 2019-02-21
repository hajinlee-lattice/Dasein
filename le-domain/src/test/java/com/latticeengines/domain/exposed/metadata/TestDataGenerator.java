package com.latticeengines.domain.exposed.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.latticeengines.domain.exposed.metadata.statistics.CategoryTopNTree;
import com.latticeengines.domain.exposed.metadata.statistics.TopAttribute;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.query.AttributeLookup;

public class TestDataGenerator {
    public static final String TEST_SUB_CATEGORY = "TestSubCateghory";
    private static final String INTERNAL_ATTR_PREFIX = "InternalAttr_";
    public static final String EXTERNAL_ATTR_PREFIX = "ExternalAttr_";

    /**
     * Generate a list of {@link ColumnMetadata} with given number of attributes
     * that can and cannot be internal enriched
     * @param internalAttributeCount number of attributes that can be internal enriched
     * @param externalAttributeCount number of attributes that cannot be internal enriched
     * @return generated list
     */
    public static List<ColumnMetadata> generateColumnMetadataList(int internalAttributeCount, int externalAttributeCount) {
        List<ColumnMetadata> list = new ArrayList<>();
        for (int i = 0; i < internalAttributeCount; i++) {
            ColumnMetadata meta = new ColumnMetadata();
            meta.setCanInternalEnrich(true);
            meta.setAttrName(INTERNAL_ATTR_PREFIX + i);
            list.add(meta);
        }
        for (int i = 0; i < externalAttributeCount; i++) {
            ColumnMetadata meta = new ColumnMetadata();
            meta.setCanInternalEnrich(false);
            meta.setAttrName(EXTERNAL_ATTR_PREFIX + i);
            list.add(meta);
        }
        Collections.shuffle(list, new Random(System.currentTimeMillis()));
        return list;
    }

    /**
     * Generate a new {@link TopNTree} with the given {@link ColumnMetadata} list
     *
     * @param cols input {@link ColumnMetadata} list
     * @return configured {@link TopNTree} object
     */
    public static TopNTree generateTopNTree(List<ColumnMetadata> cols) {
        TopNTree tree = new TopNTree();
        List<TopAttribute> attrs = new ArrayList<>();
        for (ColumnMetadata meta : cols) {
            AttributeLookup lookup = new AttributeLookup();
            lookup.setAttribute(meta.getAttrName());
            TopAttribute attr = new TopAttribute(lookup, 0L);
            attrs.add(attr);
        }

        CategoryTopNTree categoryTopNTree = new CategoryTopNTree();
        categoryTopNTree.putSubcategory(TEST_SUB_CATEGORY, attrs);
        Map<Category, CategoryTopNTree> categoryTopNTreeMap = new HashMap<>();
        categoryTopNTreeMap.put(Category.DEFAULT, categoryTopNTree);
        tree.setCategories(categoryTopNTreeMap);
        return tree;
    }
}
