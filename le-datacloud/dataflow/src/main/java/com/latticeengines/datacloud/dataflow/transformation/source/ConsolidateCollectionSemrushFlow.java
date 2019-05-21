package com.latticeengines.datacloud.dataflow.transformation.source;

import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.ConsolidateCollectionParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component(ConsolidateCollectionSemrushFlow.BEAN_NAME)
public class ConsolidateCollectionSemrushFlow extends ConsolidateCollectionFlow {
    public static final String BEAN_NAME = "consolidateCollectionSemrushFlow";
    private static final String FIELD_RANK = "Rank";
    private static final String FIELD_URL = "WebPageUrl";
    private static final String FIELD_DOMAIN = "Domain";
    private static final String ENC_API_KEY = "bi0mpJJNxiYpEka5C6JO4n0mZUMYBxS6Dz/hdEUxr0j2H45NF6Q3da1OYA75x9DjM4ryf7KlE0Xh0dyHg+Mk1VhmWicmucgVZz+g5ABxAuo=";
    private static final String URL_TEMPLATE1 = "\"http://api.semrush.com/?type=domain_rank&key=%s&export_columns=Dn," +
            "Rk,Or,Ot,Oc,Ad,At,Ac&domain=\" + ";
    private static final String URL_TEMPLATE2 = " + \"&database=us\"";

    private Node addWebPageUrlField(Node node) {
        String apiKey = CipherUtils.decrypt(ENC_API_KEY);
        String exp = String.format(URL_TEMPLATE1, apiKey)  + FIELD_DOMAIN + URL_TEMPLATE2;
        return node.apply(exp, new FieldList(FIELD_DOMAIN), new FieldMetadata(FIELD_URL, String.class));
    }

    private Node removeUnusedField(Node node) {
        List<String> fields = node.getFieldNames();
        fields.removeAll(Arrays.asList("LEInternalID", "LEParentInternalID", "WebPageUrl", "Creation_Date",
                "Last_Modification_Date"));

        return node.retain(new FieldList(fields));
    }

    @Override
    public Node construct(ConsolidateCollectionParameters parameters) {
        Node input = addSource(parameters.getBaseTables().get(0));
        input = removeUnusedField(input);

        //combine legacy bw consolidated result
        if (parameters.getBaseTables().size() == 2) {

            Node legacy = addSource(parameters.getBaseTables().get(1)).retain(input.getFieldNamesArray());

            input = input.merge(legacy);

        }

        Node src = preRecentTransform(input, parameters);

        Node recent = findMostRecent(src, parameters);

        return postRecentTransform(recent, parameters);
    }

    @Override
    protected Node preRecentTransform(Node src, ConsolidateCollectionParameters parameters) {
        return src;
    }

    @Override
    protected Node postRecentTransform(Node src, ConsolidateCollectionParameters parameters) {
        src = src.apply(
                String.format("%s == 0 ? null : %s", FIELD_RANK, FIELD_RANK), new FieldList(
                        FIELD_RANK), new FieldMetadata(FIELD_RANK, Integer.class));
        return src;
    }
}
