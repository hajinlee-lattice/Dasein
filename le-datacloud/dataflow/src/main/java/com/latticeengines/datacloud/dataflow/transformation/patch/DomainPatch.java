package com.latticeengines.datacloud.dataflow.transformation.patch;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.ValidationUtils;
import com.latticeengines.common.exposed.validator.BeanValidationService;
import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.patch.DomainPatchBookAggregator;
import com.latticeengines.dataflow.runtime.cascading.propdata.patch.DomainPatchBookBuffer;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.DomainPatchConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

/**
 * Patch item with Cleanup = 0:
 * -- If Domain + DUNS already exists in AccountMasterSeedMerged, ignore
 * -- If DUNS does not exists in AccountMasterSeedMerged, ignore
 * -- If there is matched DUNS-only entry in AccountMasterSeedMerged, populate domain field
 * -- If there is already Domain + DUNS entries with matched DUNS,
 *      need to add one more row with Domain = patched domain, LE_IS_PRIMARY_LOCATION = N,
 *      LE_IS_PRIMARY_DOMAIN = N, DomainSource = Patch, other fields same as the entry of same DUNS
 * -- PatchBook could have multiple Domain patch items with different domains attached to same DUNS
 *      - could be adding new rows for all of them
 *      - could be populating domain for one DUNS-only entry and adding others as new rows
 * Patch item with Cleanup = 1:
 * -- If Domain + DUNS doesn't exists in AccountMasterSeedMerged, ignore
 * -- If there is only one entry with matched Domain + DUNS, cleanup Domain field
 * -- If there is multiple entries with matched DUNS, remove the entry of matched Domain + DUNS
 */
@Component(DomainPatch.DATAFLOW_BEAN_NAME)
public class DomainPatch extends ConfigurableFlowBase<DomainPatchConfig> {
    public static final String DATAFLOW_BEAN_NAME = "DomainPatchFlow";
    public static final String TRANSFORMER_NAME = "DomainPatcher";

    private DomainPatchConfig config;

    private static final String PATCH_DUNS = "_LATTICE_PATCH_DUNS_";
    private static final String PATCH_DOMAINS = "_LATTICE_PATCH_DOMAINS_";

    @Autowired
    private BeanValidationService beanValidationService;

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        // The base source domainPatchBook should only have PatchBook.Type =
        // Domain. Ingestion job should handle the filter
        Node domainPatchBook = addSource(parameters.getBaseTables().get(0));
        // AMSeedMerged is guaranteed to be unique in Domain + DUNS
        Node ams = addSource(parameters.getBaseTables().get(1));
        config = getTransformerConfig(parameters);
        ValidationUtils.check(beanValidationService, config, DomainPatchConfig.class.getName());

        // Retain fields: DUNS, PatchItems, Cleanup
        domainPatchBook = domainPatchBook
                .retain(new FieldList(PatchBook.COLUMN_DUNS, PatchBook.COLUMN_PATCH_ITEMS, PatchBook.COLUMN_CLEANUP));

        // Schema after aggregation: _LATTICE_PATCH_DUNS_,
        // _LATTICE_PATCH_DOMAINS_ (Unique in _LATTICE_PATCH_DUNS_)
        // eg. _LATTICE_PATCH_DUNS_="123456789",
        // _LATTICE_PATCH_DOMAINS_="{\"CLEANUP\":[\"dom1.com\",\"dom2.com\"],\"ADD\":[\"dom3.com\"]}"
        String[] fields = { PATCH_DUNS, PATCH_DOMAINS };
        List<FieldMetadata> fms = new ArrayList<>();
        fms.add(new FieldMetadata(PATCH_DUNS, String.class));
        fms.add(new FieldMetadata(PATCH_DOMAINS, String.class));
        DomainPatchBookAggregator agg = new DomainPatchBookAggregator(new Fields(fields), PATCH_DUNS, PATCH_DOMAINS);
        domainPatchBook = domainPatchBook.groupByAndAggregate(new FieldList(PatchBook.COLUMN_DUNS), agg, fms);

        // Patch domain to AMS
        List<String> amsFields = ams.getFieldNames();
        ams = ams.join(new FieldList(DataCloudConstants.AMS_ATTR_DUNS), domainPatchBook, new FieldList(PATCH_DUNS),
                JoinType.LEFT);
        DomainPatchBookBuffer buffer = new DomainPatchBookBuffer(
                new Fields(ams.getFieldNames().toArray(new String[ams.getFieldNames().size()])),
                config.getDomainSource(), PATCH_DOMAINS);
        // Divide AMSeed to 2 parts: with duns and without duns and merge later.
        // Purpose: there are roughtly 15M domain-only entries in AMSeed. If
        // directly grouping by duns, null-duns group is too large which slows
        // down the performance a lot
        Node amsWithDuns = ams
                .filter(DataCloudConstants.AMS_ATTR_DUNS + " != null", new FieldList(DataCloudConstants.AMS_ATTR_DUNS))
                .renamePipe("_AMS_WITH_DUNS_");
        Node amsWithoutDuns = ams
                .filter(DataCloudConstants.AMS_ATTR_DUNS + " == null", new FieldList(DataCloudConstants.AMS_ATTR_DUNS))
                .renamePipe("_AMS_WITHOUT_DUNS_");
        amsWithDuns = amsWithDuns.groupByAndBuffer(new FieldList(DataCloudConstants.AMS_ATTR_DUNS), buffer);
        ams = amsWithDuns.merge(amsWithoutDuns);

        // Finalize AMSeed schema
        ams = ams.retain(new FieldList(amsFields));
        return ams;
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
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return DomainPatchConfig.class;
    }
}
