package com.latticeengines.datacloud.dataflow.transformation.patch;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.patch.DomainDunsSelectFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.patch.PatchAMAttributesFunction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

/**
 * Transformer to patch account master attributes with values specified in PatchBook entries.
 */
@Component(AttributePatch.DATAFLOW_BEAN_NAME)
public class AttributePatch extends ConfigurableFlowBase<TransformerConfig> {
    public static final String DATAFLOW_BEAN_NAME = "AttributePatchFlow";
    public static final String TRANSFORMER_NAME = "AttributePatcher";

    // NOTE internal fields start with underscore to prevent conflict with account master column names
    private static final String PATCH_DUNS = DataCloudConstants.ATTR_PATCH_DUNS;
    private static final String PATCH_DOMAIN = DataCloudConstants.ATTR_PATCH_DOMAIN;
    private static final String PATCH_ITEMS = DataCloudConstants.ATTR_PATCH_ITEMS;

    private static final String AM_DOMAIN = MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.Domain);
    private static final String AM_DUNS = MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.DUNS);
    private static final String DOMAIN_ONLY_PREFIX = "_PATCH_DOMAIN_ONLY_";
    private static final String DUNS_ONLY_PREFIX = "_PATCH_DUNS_ONLY_";
    private static final String DOMAIN_DUNS_PREFIX = "_PATCH_DOMAIN_DUNS_";

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return TransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node am = addSource(parameters.getBaseTables().get(0)); // account master
        Node attrPatchBook = addSource(parameters.getBaseTables().get(1))
                .retain(new FieldList(PATCH_DOMAIN, PATCH_DUNS, PATCH_ITEMS)); // Attribute patch book entries

        FieldList patchDomainDunsFields = new FieldList(PATCH_DOMAIN, PATCH_DUNS);
        FieldList amDomainDunsFields = new FieldList(AM_DOMAIN, AM_DUNS);
        FieldList patchItemField = new FieldList(PATCH_ITEMS);

        /*
         * Split Domain only, DUNS only, Domain + DUNS patch book entries into three branches.
         * PatchItems for each branch will prefixed with different value to prevent conflict when merging.
         */
        // entries to patch all AM rows with matching domain
        Node domainOnlyBooks = attrPatchBook
                .filter(String.format("%s != null && %s == null", PATCH_DOMAIN, PATCH_DUNS), patchDomainDunsFields)
                .rename(patchItemField, domainOnly(patchItemField));
        // entries to patch all AM rows with matching DUNS
        Node dunsOnlyBooks = attrPatchBook
                .filter(String.format("%s == null && %s != null", PATCH_DOMAIN, PATCH_DUNS), patchDomainDunsFields)
                .rename(patchItemField, dunsOnly(patchItemField));
        // entries to patch THE AM row that has the matching Domain + DUNS
        Node domainDunsBooks = attrPatchBook
                .filter(String.format("%s != null && %s != null", PATCH_DOMAIN, PATCH_DUNS), patchDomainDunsFields)
                .rename(patchItemField, domainDuns(patchItemField));

        /*
         * Inner join to retrieve all AM rows that need to be patched by all branches (Domain, DUNS, Domain + DUNS).
         */
        Node domainAm = am
                .filter(String.format("%s != null", AM_DOMAIN), new FieldList(AM_DOMAIN))
                .retain(AM_DOMAIN, AM_DUNS)
                .join(AM_DOMAIN, domainOnlyBooks, PATCH_DOMAIN, JoinType.INNER)
                .discard(patchDomainDunsFields)
                // Columns = [ AM_Domain, AM_DUNS, PREFIX(PatchItems) ]
                // NOTE didn't prefix AM_Domain and AM_DUNS here because these fields will be used to store selected
                //      Domain + DUNS pair after merging all branches
                .renamePipe("DomainOnly");
        Node dunsAm = am
                .filter(String.format("%s != null", AM_DUNS), new FieldList(AM_DUNS))
                .rename(amDomainDunsFields, dunsOnly(amDomainDunsFields))
                // for field alignment (needed if rename fields before join)
                .retain(dunsOnly(AM_DOMAIN), dunsOnly(AM_DUNS))
                .join(dunsOnly(AM_DUNS), dunsOnlyBooks, PATCH_DUNS, JoinType.INNER)
                .discard(patchDomainDunsFields)
                // Columns = [ PREFIX(AM_Domain, AM_DUNS, PatchItems) ]
                .renamePipe("DunsOnly");
        Node domainDunsAm = am
                .filter(String.format("%s != null && %s != null", AM_DOMAIN, AM_DUNS), amDomainDunsFields)
                .rename(amDomainDunsFields, domainDuns(amDomainDunsFields))
                // for field alignment (needed if rename fields before join)
                .retain(domainDuns(AM_DOMAIN), domainDuns(AM_DUNS))
                .join(domainDuns(amDomainDunsFields), domainDunsBooks, patchDomainDunsFields, JoinType.INNER)
                .discard(patchDomainDunsFields)
                // Columns = [ PREFIX(AM_Domain, AM_DUNS, PatchItems) ]
                .renamePipe("DomainDuns");

        /*
         * Merge AM rows that need to be patched from all branches. After merging, copy the non-blank Domain + DUNS
         * pair to AM_Domain and AM_DUNS fields. (Domain + DUNS pair from each branch will be non-null if this AM row
         * need to be patched by that branch. Thus, we need to find one non-null pair from all branches.)
         */
        // fields required for patch entries (does not need other AM columns as they will be
        String[] fieldNames = {
                AM_DOMAIN, AM_DUNS, dunsOnly(AM_DOMAIN), dunsOnly(AM_DUNS), domainDuns(AM_DOMAIN), domainDuns(AM_DUNS),
                domainOnly(PATCH_ITEMS), dunsOnly(PATCH_ITEMS), domainDuns(PATCH_ITEMS)
        };
        FieldList fieldList = new FieldList(fieldNames);
        DomainDunsSelectFunction selectFn = new DomainDunsSelectFunction(
                new Fields(fieldNames),
                AM_DOMAIN, AM_DUNS,
                new String[] { AM_DOMAIN, dunsOnly(AM_DOMAIN), domainDuns(AM_DOMAIN) },
                new String[] { AM_DUNS, dunsOnly(AM_DUNS), domainDuns(AM_DUNS) }
        );
        Node patchEntries = domainAm
                // outer join because not all AM rows will be patched by every branch
                .coGroup(
                        amDomainDunsFields, Arrays.asList(dunsAm, domainDunsAm),
                        Arrays.asList(dunsOnly(amDomainDunsFields), domainDuns(amDomainDunsFields)),
                        JoinType.OUTER)
                // select non-null Domain + DUNS pair. outer join will cause Domain + DUNS from some branches
                // be null
                .apply(selectFn, fieldList, getStringMetadataList(fieldNames), fieldList, Fields.REPLACE)
                // discard redundant Domain + DUNS from other branches
                .discard(dunsOnly(AM_DOMAIN), dunsOnly(AM_DUNS), domainDuns(AM_DOMAIN), domainDuns(AM_DUNS))
                // rename AM_Domain & AM_DUNS for easier removal after join back to AM
                .rename(amDomainDunsFields, domainOnly(amDomainDunsFields))
                // for field alignment (needed if rename fields before join)
                .retain(domainOnly(AM_DOMAIN), domainOnly(AM_DUNS),
                        domainOnly(PATCH_ITEMS), dunsOnly(PATCH_ITEMS), domainDuns(PATCH_ITEMS));

        /*
         * Join patchEntries back to AM (patchItems fields will be appended) to prepare for patching attributes
         *
         * Columns = [ AM cols, domainOnly(PATCH_ITEMS), dunsOnly(PATCH_ITEMS), domainDuns(PATCH_ITEMS) ]
         */
        Node amWithPatchItems = am
                .join(amDomainDunsFields, patchEntries, domainOnly(amDomainDunsFields), JoinType.LEFT)
                .retain(ArrayUtils.addAll(
                        am.getFieldNamesArray(),
                        domainOnly(AM_DOMAIN), domainOnly(AM_DUNS),
                        domainOnly(PATCH_ITEMS), dunsOnly(PATCH_ITEMS), domainDuns(PATCH_ITEMS)))
                .discard(domainOnly(amDomainDunsFields));

        /*
         * Patch attributes
         *
         * Columns = [ AM cols ]
         */
        return amWithPatchItems
                .apply(
                        getPatchAttrFunction(amWithPatchItems), new FieldList(amWithPatchItems.getFieldNames()),
                        amWithPatchItems.getSchema(), new FieldList(amWithPatchItems.getFieldNames()), Fields.REPLACE)
                .discard(domainOnly(PATCH_ITEMS), dunsOnly(PATCH_ITEMS), domainDuns(PATCH_ITEMS));
    }


    /*
     * Generate functions for patching attributes using patchItems from all branches
     */
    private PatchAMAttributesFunction getPatchAttrFunction(Node amWithPatchItems) {
        String[] fieldNames = amWithPatchItems
                .getSchema()
                .stream()
                .map(FieldMetadata::getFieldName)
                .toArray(String[]::new);
        Type[] fieldTypes = amWithPatchItems
                .getSchema()
                .stream()
                .map(FieldMetadata::getJavaType)
                .toArray(Type[]::new);
        return new PatchAMAttributesFunction(
                new Fields(fieldNames, fieldTypes),
                new String[] { domainOnly(PATCH_ITEMS), dunsOnly(PATCH_ITEMS), domainDuns(PATCH_ITEMS) });
    }

    /*
     * helpers to add prefix for domain only, duns only and domain + duns patch book entries to prevent conflict
     * with account master column names
     */

    private String domainOnly(String key) {
        return DOMAIN_ONLY_PREFIX + key;
    }

    private FieldList domainOnly(FieldList fieldList) {
        return prefix(DOMAIN_ONLY_PREFIX, fieldList);
    }

    private String dunsOnly(String key) {
        return DUNS_ONLY_PREFIX + key;
    }

    private FieldList dunsOnly(FieldList fieldList) {
        return prefix(DUNS_ONLY_PREFIX, fieldList);
    }

    private String domainDuns(String key) {
        return DOMAIN_DUNS_PREFIX + key;
    }

    private FieldList domainDuns(FieldList fieldList) {
        return prefix(DOMAIN_DUNS_PREFIX, fieldList);
    }

    private FieldList prefix(String prefix, FieldList list) {
        String[] fields = list.getFieldsAsList().stream().map(field -> prefix + field).toArray(String[]::new);
        return new FieldList(fields);
    }

    /*
     * return a list of field metadata with the input field names and string type
     */
    private List<FieldMetadata> getStringMetadataList(String... fieldNames) {
        return Arrays
                .stream(fieldNames)
                .map(name -> new FieldMetadata(name, String.class))
                .collect(Collectors.toList());
    }
}
