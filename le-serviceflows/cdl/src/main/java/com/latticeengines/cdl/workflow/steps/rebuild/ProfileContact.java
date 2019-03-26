package com.latticeengines.cdl.workflow.steps.rebuild;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableList;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;

@Component(ProfileContact.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProfileContact extends BaseSingleEntityProfileStep<ProcessContactStepConfiguration> {

    static final String BEAN_NAME = "profileContact";

    private static final Logger log = LoggerFactory.getLogger(ProfileContact.class);

    private List<String> dedupFields = ImmutableList.of(InterfaceName.AccountId.name());

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        setEvaluationDateStrAndTimestamp();
    }

    @Override
    protected TableRoleInCollection profileTableRole() {
        return TableRoleInCollection.ContactProfile;
    }

    @Override
    protected PipelineTransformationRequest getTransformRequest() {
        String masterTableName = masterTable.getName();

        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("ProfileContactStep");
        request.setSubmitter(customerSpace.getTenantId());
        request.setKeepTemp(false);
        request.setEnableSlack(false);

        int profileStep = 0;
        int bucketStep = 1;

        TransformationStepConfig profile = profile(masterTableName);
        TransformationStepConfig bucket = bucket(profileStep, masterTableName, servingStoreTablePrefix);
        TransformationStepConfig calc = calcStats(profileStep, bucketStep, statsTablePrefix, dedupFields);
        TransformationStepConfig sortProfile = sort(profileStep, profileTablePrefix,
                DataCloudConstants.PROFILE_ATTR_ATTRNAME, 1);
        // -----------
        List<TransformationStepConfig> steps = Arrays.asList( //
                profile, //
                bucket, //
                calc, //
                sortProfile //
        );
        // -----------
        request.setSteps(steps);
        return request;
    }

    @Override
    protected void enrichTableSchema(Table table) {
        Map<String, Attribute> masterAttrs = new HashMap<>();
        masterTable.getAttributes().forEach(attr -> {
            masterAttrs.put(attr.getName(), attr);
        });
        List<Attribute> attrs = new ArrayList<>();
        final AtomicLong masterCount = new AtomicLong(0);
        table.getAttributes().forEach(attr0 -> {
            Attribute attr = copyMasterAttr(masterAttrs, attr0);
            if (masterAttrs.containsKey(attr0.getName())) {
                attr = copyMasterAttr(masterAttrs, attr0);
                if (LogicalDataType.Date.equals(attr0.getLogicalDataType())) {
                    log.info("Setting last data refresh for contact date attribute: " + attr.getName() + " to "
                            + evaluationDateStr);
                    attr.setLastDataRefresh("Last Data Refresh: " + evaluationDateStr);
                }
                masterCount.incrementAndGet();
            }
            // update metadata for AccountId attribute since it is only created after lead
            // to account match and does not have the correct metadata
            if (configuration.isEntityMatchEnabled() && InterfaceName.AccountId.name().equals(attr.getName())) {
                attr.setInterfaceName(InterfaceName.AccountId);
                attr.setTags(Tag.INTERNAL);
                attr.setLogicalDataType(LogicalDataType.Id);
                attr.setNullable(false);
                attr.setApprovedUsage(ApprovedUsage.NONE);
                attr.setSourceLogicalDataType(attr.getPhysicalDataType());
                attr.setFundamentalType(FundamentalType.ALPHA.getName());
            }
            attr.setCategory(Category.CONTACT_ATTRIBUTES);
            attr.setSubcategory(null);
            attr.removeAllowedDisplayNames();
            attrs.add(attr);
        });
        table.setAttributes(attrs);
        log.info("Copied " + masterCount.get() + " attributes from batch store metadata.");
    }

}
