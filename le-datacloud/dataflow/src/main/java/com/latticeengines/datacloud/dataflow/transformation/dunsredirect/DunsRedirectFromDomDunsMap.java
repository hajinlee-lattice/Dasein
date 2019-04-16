package com.latticeengines.datacloud.dataflow.transformation.dunsredirect;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.DunsRedirectFromDomDunsMapFunc;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.DunsRedirectBookConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

/**
 * For each domain which attached to multiple duns, 
 *      there are IsPrimaryLocation, IsPrimaryCountryLocation and IsPrimaryStateLocation flags selected.
 * For duns with IsPrimaryLocation = true, treat it as global primary duns for that domain. 
 *      For name only match input, redirect other duns attached to that domain, to this global primary duns.
 * For duns with IsPrimaryCountryLocation = true, treat it as country primary duns for that domain. 
 *      For name + country match input, redirect other duns attached to that domain and with same country, to this country primary duns. 
 * For duns with IsPrimaryStateLocation = true, treat it as state primary duns for that domain. 
 *      For name + country + state match input, redirect other duns attached to that domain and with same country + state, to this state primary duns.
 *
 */
@Component(DunsRedirectFromDomDunsMap.DATAFLOW_BEAN_NAME)
public class DunsRedirectFromDomDunsMap extends ConfigurableFlowBase<DunsRedirectBookConfig> {
    public static final String DATAFLOW_BEAN_NAME = "DunsRedirectFromDomDunsMap";
    public static final String TRANSFORMER_NAME = "DunsRedirectorFromDomDunsMap";

    private static final String PRIMARY_DUNS = "PrimaryDuns";
    private static final String PRIMARY_CTRY_DUNS = "PrimaryCtryDuns";
    private static final String PRIMARY_ST_DUNS = "PrimaryStDuns";

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(DunsRedirectFromDomDunsMap.class);

    private DunsRedirectBookConfig config;

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        config = getTransformerConfig(parameters);
        Node ams = addSource(parameters.getBaseTables().get(0));

        Node priDunsAttached = attachPrimaryDuns(ams);
        return buildBook(priDunsAttached);
    }

    private Node attachPrimaryDuns(Node ams) {
        ams = ams.filter(
                String.format("%s != null && %s != null", DataCloudConstants.ATTR_LDC_DOMAIN,
                        DataCloudConstants.ATTR_LDC_DUNS),
                new FieldList(DataCloudConstants.ATTR_LDC_DOMAIN, DataCloudConstants.ATTR_LDC_DUNS))
                .retain(new FieldList(DataCloudConstants.ATTR_LDC_DOMAIN, DataCloudConstants.ATTR_LDC_DUNS,
                        DataCloudConstants.ATTR_COUNTRY, DataCloudConstants.ATTR_STATE,
                        DataCloudConstants.ATTR_IS_PRIMARY_LOCATION, DataCloudConstants.ATTR_IS_CTRY_PRIMARY_LOCATION,
                        DataCloudConstants.ATTR_IS_ST_PRIMARY_LOCATION));

        // In AMSeed, for each domain, there is only one duns having
        // LE_IS_PRIMARY_LOCATION = true
        // Schema: LDC_Domain, PrimaryDuns
        Node priDuns = ams.filter(
                String.format("(\"Y\").equals(%s)", DataCloudConstants.ATTR_IS_PRIMARY_LOCATION),
                new FieldList(DataCloudConstants.ATTR_IS_PRIMARY_LOCATION))
                .retain(new FieldList(DataCloudConstants.ATTR_LDC_DOMAIN, DataCloudConstants.ATTR_LDC_DUNS))
                .rename(new FieldList(DataCloudConstants.ATTR_LDC_DUNS), new FieldList(PRIMARY_DUNS));
        // Use retain to avoid potential column alignment issue
        priDuns = priDuns.retain(new FieldList(priDuns.getFieldNames())).renamePipe("PrimaryDuns");
        // In AMSeed, for each domain + country, there is only one duns having
        // IsCountryPrimaryLocation = true
        // Schema: LDC_Domain, LDC_Country, PrimaryCtryDuns
        Node priCtryDuns = ams
                .filter(String.format("(\"Y\").equals(%s) && %s != null",
                        DataCloudConstants.ATTR_IS_CTRY_PRIMARY_LOCATION, DataCloudConstants.ATTR_COUNTRY),
                        new FieldList(DataCloudConstants.ATTR_IS_CTRY_PRIMARY_LOCATION,
                                DataCloudConstants.ATTR_COUNTRY))
                .retain(new FieldList(DataCloudConstants.ATTR_LDC_DOMAIN, DataCloudConstants.ATTR_LDC_DUNS,
                        DataCloudConstants.ATTR_COUNTRY))
                .rename(new FieldList(DataCloudConstants.ATTR_LDC_DUNS), new FieldList(PRIMARY_CTRY_DUNS));
        // Use retain to avoid potential column alignment issue
        priCtryDuns = priCtryDuns.retain(new FieldList(priCtryDuns.getFieldNames())).renamePipe("PrimaryCtryDuns");
        // In AMSeed, for each domain + country + state, there is only one duns
        // having IsStatePrimaryLocation = true
        // Schema: LDC_Domain, LDC_Country, LDC_State, PrimaryStDuns
        Node priStDuns = ams
                .filter(String.format("(\"Y\").equals(%s) && %s != null && %s != null",
                        DataCloudConstants.ATTR_IS_ST_PRIMARY_LOCATION, DataCloudConstants.ATTR_COUNTRY,
                        DataCloudConstants.ATTR_STATE),
                        new FieldList(DataCloudConstants.ATTR_IS_ST_PRIMARY_LOCATION, DataCloudConstants.ATTR_COUNTRY,
                                DataCloudConstants.ATTR_STATE))
                .retain(new FieldList(DataCloudConstants.ATTR_LDC_DOMAIN, DataCloudConstants.ATTR_LDC_DUNS,
                        DataCloudConstants.ATTR_COUNTRY, DataCloudConstants.ATTR_STATE))
                .rename(new FieldList(DataCloudConstants.ATTR_LDC_DUNS), new FieldList(PRIMARY_ST_DUNS));
        // Use retain to avoid potential column alignment issue
        priStDuns = priStDuns.retain(new FieldList(priStDuns.getFieldNames())).renamePipe("PrimaryStDuns");

        // Attach primary duns to domain + duns from AMSeed
        // Schema: LDC_Domain, LDC_Duns, PrimaryDuns, PrimaryCtryDuns,
        // PrimaryStDuns
        Node priDunsAttached = ams
                .join(new FieldList(DataCloudConstants.ATTR_LDC_DOMAIN), priDuns,
                        new FieldList(DataCloudConstants.ATTR_LDC_DOMAIN), JoinType.LEFT)
                .join(new FieldList(DataCloudConstants.ATTR_LDC_DOMAIN, DataCloudConstants.ATTR_COUNTRY),
                        priCtryDuns,
                        new FieldList(DataCloudConstants.ATTR_LDC_DOMAIN, DataCloudConstants.ATTR_COUNTRY),
                        JoinType.LEFT)
                .join(new FieldList(DataCloudConstants.ATTR_LDC_DOMAIN, DataCloudConstants.ATTR_COUNTRY,
                        DataCloudConstants.ATTR_STATE), priStDuns,
                        new FieldList(DataCloudConstants.ATTR_LDC_DOMAIN, DataCloudConstants.ATTR_COUNTRY,
                                DataCloudConstants.ATTR_STATE),
                        JoinType.LEFT)
                .retain(new FieldList(DataCloudConstants.ATTR_LDC_DUNS, PRIMARY_DUNS, PRIMARY_CTRY_DUNS,
                        PRIMARY_ST_DUNS));
        return priDunsAttached;
    }

    private Node buildBook(Node priDunsAttached) {
        List<FieldMetadata> targetMetadata = new ArrayList<>();
        targetMetadata.add(new FieldMetadata(DunsRedirectBookConfig.DUNS, String.class));
        targetMetadata.add(new FieldMetadata(DunsRedirectBookConfig.TARGET_DUNS, String.class));
        targetMetadata.add(new FieldMetadata(DunsRedirectBookConfig.KEY_PARTITION, String.class));
        targetMetadata.add(new FieldMetadata(DunsRedirectBookConfig.BOOK_SOURCE, String.class));
        String[] outputFields = { DunsRedirectBookConfig.DUNS, DunsRedirectBookConfig.TARGET_DUNS,
                DunsRedirectBookConfig.KEY_PARTITION, DunsRedirectBookConfig.BOOK_SOURCE };
        Node book = priDunsAttached.apply(
                new DunsRedirectFromDomDunsMapFunc(new Fields(outputFields), config.getBookSource(), PRIMARY_DUNS,
                        PRIMARY_CTRY_DUNS, PRIMARY_ST_DUNS),
                new FieldList(priDunsAttached.getFieldNames()), targetMetadata, new FieldList(outputFields));
        return book;
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
        return DunsRedirectBookConfig.class;
    }
}
