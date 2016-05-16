package com.latticeengines.transform.v2_0_25.functions;

import java.util.LinkedHashMap;
import java.util.Map;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.ApprovedUsage;
import com.latticeengines.transform.exposed.metadata.Category;
import com.latticeengines.transform.exposed.metadata.FundamentalType;
import com.latticeengines.transform.exposed.metadata.StatisticalType;
import com.latticeengines.transform.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;
import com.latticeengines.transform.v2_0_25.common.DSUtils;

public class StdVisidbDsTitleFunction implements RealTimeTransform {

    private static final long serialVersionUID = -8208353638753982691L;
    @SuppressWarnings("rawtypes")
    private static LinkedHashMap mapTitleFunction = null;

    public StdVisidbDsTitleFunction() {

    }

    public StdVisidbDsTitleFunction(String modelPath) {

    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object o = record.get(column);

        if (o == null)
            return "Null";

        if (mapTitleFunction == null) {
            mapTitleFunction = new LinkedHashMap();
            mapTitleFunction.put("IT", "it,information technology,database,network,middleware,security");
            mapTitleFunction.put("Engineering", "quality,system,engineer,develope,software,testing,unix,linux,product");
            mapTitleFunction.put("Finance", "financ,accounting,treasurer,tax,loan,risk,purchasing");
            mapTitleFunction.put("Marketing",
                    "market,interactive,advertis,brand,mktg,content,commerc,social,generat,event,media");
            mapTitleFunction.put("Sales", "sale,channel,crm,field,inside");
            mapTitleFunction.put("Human Resource", "human resource,hr,talent,benefits");
            mapTitleFunction.put("Operations", "warehouse,operat,strateg,plan,distribut,chain,facilit");
            mapTitleFunction.put("Public Relations", "communication,public,affairs,relation,community,pr");
            mapTitleFunction.put("Design", "creative,design,art,designer");
            mapTitleFunction.put("Services",
                    "service, solution, training, implementation, user, help, care, maintenance, engage,"
                            + "instruction, account manager, account executive,soa");
            mapTitleFunction.put("Research", "research");
            mapTitleFunction.put("Analytics", "data,analy");
            mapTitleFunction.put("Architect", "architect");
            mapTitleFunction.put("Academic", "education,academi");
            mapTitleFunction.put("Consulting", "consult");
        }

        String s = (String) o;

        return DSUtils.valueReturn(s, mapTitleFunction);
    }

    @Override
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        metadata.setCategory(Category.LEAD_INFORMATION);
        metadata.setFundamentalType(FundamentalType.ALPHA);
        metadata.setStatisticalType(StatisticalType.NOMINAL);
        metadata.setDescription("Title Function");
        metadata.setDisplayName("Title Function");
        metadata.setTags(Tag.INTERNAL_TRANSFORM);
        return metadata;
    }

}
