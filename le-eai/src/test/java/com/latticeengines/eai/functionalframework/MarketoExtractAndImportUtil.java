package com.latticeengines.eai.functionalframework;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;

public class MarketoExtractAndImportUtil {

    public static Table createMarketoActivity() {
        Table table = new Table();
        table.setName("Activity");
        Attribute id = new Attribute();
        id.setName("id");
        id.setDisplayName("Id");
        id.setLogicalDataType("String");
        Attribute leadId = new Attribute();
        leadId.setName("leadId");
        leadId.setDisplayName("Lead Id");
        leadId.setLogicalDataType("Int");
        Attribute activityDate = new Attribute();
        activityDate.setName("activityDate");
        activityDate.setDisplayName("Activity Date");
        activityDate.setLogicalDataType("Timestamp");
        Attribute activityTypeId = new Attribute();
        activityTypeId.setName("activityTypeId");
        activityTypeId.setDisplayName("Activity Type Id");
        activityTypeId.setLogicalDataType("Int");
        table.addAttribute(id);
        table.addAttribute(leadId);
        table.addAttribute(activityDate);
        table.addAttribute(activityTypeId);
        
        PrimaryKey pk = new PrimaryKey();
        pk.setName("PK_ID");
        pk.addAttribute(id);
        table.setPrimaryKey(pk);
        
        LastModifiedKey lk = new LastModifiedKey();
        lk.setName("LK_ActivityDate");
        lk.addAttribute(activityDate);
        table.setLastModifiedKey(lk);
        return table;
    }

    public static Table createMarketoActivityType() {
        Table table = new Table();
        table.setName("ActivityType");
        Attribute id = new Attribute();
        id.setName("id");
        id.setDisplayName("Id");
        id.setLogicalDataType("String");

        Attribute name = new Attribute();
        name.setName("name");
        name.setDisplayName("Name");
        name.setLogicalDataType("String");

        Attribute description = new Attribute();
        description.setName("description");
        description.setDisplayName("Description");
        description.setLogicalDataType("String");

        Attribute attributes = new Attribute();
        attributes.setName("attributes");
        attributes.setDisplayName("Attributes");
        attributes.setLogicalDataType("String");

        table.addAttribute(id);
        table.addAttribute(name);
        table.addAttribute(description);
        table.addAttribute(attributes);

        PrimaryKey pk = new PrimaryKey();
        pk.setName("PK_ID");
        pk.addAttribute(id);
        table.setPrimaryKey(pk);
        
        return table;
    }

    public static Table createMarketoLead() {
        Table table = new Table();
        table.setName("Lead");

        Attribute id = new Attribute();
        id.setName("id");
        Attribute anonymousIP = new Attribute();
        anonymousIP.setName("anonymousIP");
        Attribute inferredCompany = new Attribute();
        inferredCompany.setName("inferredCompany");
        Attribute inferredCountry = new Attribute();
        inferredCountry.setName("inferredCountry");
        Attribute title = new Attribute();
        title.setName("title");
        Attribute department = new Attribute();
        department.setName("department");
        Attribute unsubscribed = new Attribute();
        unsubscribed.setName("unsubscribed");
        Attribute unsubscribedReason = new Attribute();
        unsubscribedReason.setName("unsubscribedReason");
        Attribute doNotCall = new Attribute();
        doNotCall.setName("doNotCall");
        Attribute country = new Attribute();
        country.setName("country");
        Attribute website = new Attribute();
        website.setName("website");
        Attribute email = new Attribute();
        email.setName("email");
        Attribute leadStatus = new Attribute();
        leadStatus.setName("leadStatus");
        Attribute company = new Attribute();
        company.setName("company");
        Attribute leadSource = new Attribute();
        leadSource.setName("leadSource");
        Attribute industry = new Attribute();
        industry.setName("industry");
        Attribute annualRevenue = new Attribute();
        annualRevenue.setName("annualRevenue");
        Attribute numEmployees = new Attribute();
        numEmployees.setName("numberOfEmployees");
        Attribute doNotCallReason = new Attribute();
        doNotCallReason.setName("doNotCallReason");
        Attribute sicCode = new Attribute();
        sicCode.setName("sicCode");
        Attribute phone = new Attribute();
        phone.setName("phone");
        Attribute facebookReferredEnrollments = new Attribute();
        facebookReferredEnrollments.setName("facebookReferredEnrollments");
        Attribute facebookReferredVisits = new Attribute();
        facebookReferredVisits.setName("facebookReferredVisits");

        table.addAttribute(id);
        table.addAttribute(anonymousIP);
        table.addAttribute(inferredCompany);
        table.addAttribute(inferredCountry);
        table.addAttribute(title);
        table.addAttribute(department);
        table.addAttribute(unsubscribed);
        table.addAttribute(unsubscribedReason);
        table.addAttribute(doNotCall);
        table.addAttribute(country);
        table.addAttribute(website);
        table.addAttribute(email);
        table.addAttribute(leadStatus);
        table.addAttribute(company);
        table.addAttribute(leadSource);
        table.addAttribute(industry);
        table.addAttribute(annualRevenue);
        table.addAttribute(numEmployees);
        table.addAttribute(doNotCallReason);
        table.addAttribute(sicCode);
        table.addAttribute(phone);
        table.addAttribute(facebookReferredEnrollments);
        table.addAttribute(facebookReferredVisits);

        PrimaryKey pk = new PrimaryKey();
        pk.setName("PK_ID");
        pk.addAttribute(id);
        table.setPrimaryKey(pk);
        
        return table;
    }

}
