package com.latticeengines.leadprioritization.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.flows.AddStandardAttributesParameters;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

@Component("addStandardAttributes")
public class AddStandardAttributes extends TypesafeDataFlowBuilder<AddStandardAttributesParameters> {

    private static final Logger log = Logger.getLogger(AddStandardAttributes.class);

    @Override
    public Node construct(AddStandardAttributesParameters parameters) {
        Node eventTable = addSource(parameters.eventTable);
        Node last = eventTable;

        Attribute emailOrWebsite = eventTable.getSourceAttribute(InterfaceName.Email) != null //
        ? eventTable.getSourceAttribute(InterfaceName.Email) //
                : eventTable.getSourceAttribute(InterfaceName.Website);

        FieldMetadata fm;

        fm = new FieldMetadata("CompanyName_Entropy", Double.class);
        last = addFunction(last, "std_visidb_ds_companyname_entropy", fm,
                eventTable.getSourceAttribute(InterfaceName.CompanyName));

        fm = new FieldMetadata("Title_Length", Integer.class);
        last = addFunction(last, "std_length", fm, //
                eventTable.getSourceAttribute(InterfaceName.Title));

        fm = new FieldMetadata("CompanyName_Length", Integer.class);
        last = addFunction(last, "std_length", fm, //
                eventTable.getSourceAttribute(InterfaceName.CompanyName));

        fm = new FieldMetadata("RelatedLinks_Count", Integer.class);
        last = addFunction(last, "std_visidb_ds_pd_alexa_relatedlinks_count", fm, //
                eventTable.getSourceAttribute("AlexaRelatedLinks"));

        fm = new FieldMetadata("Domain_Length", Integer.class);
        last = addFunction(last, "std_length", fm, emailOrWebsite);

        fm = new FieldMetadata("Phone_Entropy", Double.class);
        last = addFunction(last, "std_entropy", fm, //
                eventTable.getSourceAttribute(InterfaceName.PhoneNumber));

        fm = new FieldMetadata("Website_Age_Months", Long.class);
        last = addFunction(last, "std_visidb_alexa_monthssinceonline", fm, //
                eventTable.getSourceAttribute("AlexaOnlineSince"));

        fm = new FieldMetadata("ModelAction1", Integer.class);
        last = addFunction(last, "std_visidb_ds_pd_modelaction_ordered", fm, //
                eventTable.getSourceAttribute("ModelAction"));

        fm = new FieldMetadata("SpamIndicator", Integer.class);
        last = addFunction(last, "std_visidb_ds_spamindicator", fm, //
                eventTable.getSourceAttribute(InterfaceName.FirstName), //
                eventTable.getSourceAttribute(InterfaceName.LastName), //
                eventTable.getSourceAttribute(InterfaceName.Title), //
                eventTable.getSourceAttribute(InterfaceName.PhoneNumber), //
                eventTable.getSourceAttribute(InterfaceName.CompanyName));

        fm = new FieldMetadata("Title_Level", Integer.class);
        last = addFunction(last, "std_visidb_ds_title_level", fm, //
                eventTable.getSourceAttribute(InterfaceName.Title));

        fm = new FieldMetadata("Title_IsTechRelated", Boolean.class);
        last = addFunction(last, "std_visidb_ds_title_istechrelated", fm, //
                eventTable.getSourceAttribute(InterfaceName.Title));

        fm = new FieldMetadata("JobsTrendString1", Integer.class);
        last = addFunction(last, "std_visidb_ds_pd_jobstrendstring_ordered", fm, //
                eventTable.getSourceAttribute("JobsTrendString"));

        fm = new FieldMetadata("FundingStage1", Integer.class);
        last = addFunction(last, "std_visidb_ds_pd_fundingstage_ordered", fm, //
                eventTable.getSourceAttribute("FundingStage"));

        fm = new FieldMetadata("Title_IsAcademic", Boolean.class);
        last = addFunction(last, "std_visidb_ds_title_isacademic", fm, //
                eventTable.getSourceAttribute(InterfaceName.Title));

        fm = new FieldMetadata("FirstName_SameAs_LastName", Boolean.class);
        last = addFunction(last, "std_visidb_ds_firstname_sameas_lastname", fm,
                eventTable.getSourceAttribute(InterfaceName.FirstName),
                eventTable.getSourceAttribute(InterfaceName.LastName));

        fm = new FieldMetadata("Industry_Group", String.class);
        last = addFunction(last, "std_visidb_ds_industry_group", fm, //
                eventTable.getSourceAttribute(InterfaceName.Industry));

        return last;
    }

    private Node addFunction(Node last, String method, FieldMetadata fm, Attribute... sourceAttributes) {
        List<String> fields = new ArrayList<>();

        for (Attribute attribute : sourceAttributes) {
            if (attribute == null) {
                log.info(String.format(
                        "Excluding field %s (function %s) because some source columns are not available",
                        fm.getFieldName(), method));
                return last;
            }
            fields.add(attribute.getName());
        }
        return last.addJythonFunction("com.latticeengines.serviceflows.core.transforms", method, method, //
                new FieldList(fields), fm);
    }
}
