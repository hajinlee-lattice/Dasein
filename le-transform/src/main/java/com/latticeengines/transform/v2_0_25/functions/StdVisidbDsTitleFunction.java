package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import org.apache.commons.collections.OrderedMap;
import org.apache.commons.collections.map.LinkedMap;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.v2_0_25.common.DSUtils;

public class StdVisidbDsTitleFunction implements RealTimeTransform {

    private static final long serialVersionUID = -8208353638753982691L;
    private static OrderedMap mapTitleFunction = null;

    public StdVisidbDsTitleFunction(String modelPath) {

    }

    @SuppressWarnings("unchecked")
    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object o = record.get(column);

        if (o == null)
            return "Null";

        if (mapTitleFunction == null) {
            mapTitleFunction = new LinkedMap();
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
    public Attribute getMetadata() {
        // TODO Auto-generated method stub
        return null;
    }

}
