package com.latticeengines.cdl.workflow.steps.play;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.play.PlayLaunchContext.Counter;
import com.latticeengines.cdl.workflow.steps.play.PlayLaunchContext.PlayLaunchContextBuilder;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.RestrictionBuilder;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.DeltaCampaignLaunchInitStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.CampaignLaunchInitStepConfiguration;
import com.latticeengines.domain.exposed.util.ChannelConfigUtil;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.ExportFieldMetadataProxy;
import com.latticeengines.proxy.exposed.cdl.LookupIdMappingProxy;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("campaignLaunchProcessor")
public class CampaignLaunchProcessor {

    private static final Logger log = LoggerFactory.getLogger(CampaignLaunchProcessor.class);

    @Inject
    private FrontEndQueryCreator frontEndQueryCreator;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private LookupIdMappingProxy lookupIdMappingProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Value("${yarn.pls.url}")
    private String internalResourceHostPort;

    @Inject
    private PlayProxy playProxy;

    @Inject
    private ExportFieldMetadataProxy exportFieldMetadataProxy;

    public ProcessedFieldMappingMetadata prepareFrontEndQueries(PlayLaunchContext playLaunchContext,
            DataCollection.Version version) {
        // prepare basic account and contact front end queries
        ProcessedFieldMappingMetadata result = frontEndQueryCreator.prepareFrontEndQueries(playLaunchContext, true);
        applyFiltersToQueries(playLaunchContext);
        handleLookupIdBasedSuppression(playLaunchContext);
        return result;
    }

    private void handleLookupIdBasedSuppression(PlayLaunchContext playLaunchContext) {
        // do handling of SFDC id based suppression
        PlayLaunch launch = playLaunchContext.getPlayLaunch();
        if (launch.getExcludeItemsWithoutSalesforceId()) {
            FrontEndQuery accountFrontEndQuery = playLaunchContext.getAccountFrontEndQuery();

            Restriction accountRestriction = accountFrontEndQuery.getAccountRestriction().getRestriction();
            Restriction nonNullLookupIdRestriction = Restriction.builder()
                    .let(BusinessEntity.Account, launch.getDestinationAccountId()).isNotNull().build();

            Restriction accountRestrictionWithNonNullLookupId = Restriction.builder()
                    .and(accountRestriction, nonNullLookupIdRestriction).build();
            accountFrontEndQuery.getAccountRestriction().setRestriction(accountRestrictionWithNonNullLookupId);

        }
    }

    private void applyFiltersToQueries(PlayLaunchContext playLaunchContext) {
        PlayLaunch launch = playLaunchContext.getPlayLaunch();
        LookupIdMap lookupIdMap = lookupIdMappingProxy.getLookupIdMapByOrgId(playLaunchContext.getTenant().getId(),
                launch.getDestinationOrgId(), launch.getDestinationSysType());
        CDLExternalSystemName destinationSystemName = lookupIdMap.getExternalSystemName();
        if (launch.getChannelConfig().isSuppressContactsWithoutEmails()) {
            FrontEndQuery accountFrontEndQuery = playLaunchContext.getAccountFrontEndQuery();
            Restriction newContactRestrictionForAccountQuery = applyEmailFilterToContactRestriction(
                    accountFrontEndQuery.getContactRestriction().getRestriction());
            accountFrontEndQuery.setContactRestriction(new FrontEndRestriction(newContactRestrictionForAccountQuery));

            FrontEndQuery contactFrontEndQuery = playLaunchContext.getContactFrontEndQuery();
            Restriction newContactRestrictionForContactQuery = applyEmailFilterToContactRestriction(
                    contactFrontEndQuery.getContactRestriction().getRestriction());
            contactFrontEndQuery.setContactRestriction(new FrontEndRestriction(newContactRestrictionForContactQuery));
        }

        if (ChannelConfigUtil.shouldApplyAccountNameOrWebsiteFilter(destinationSystemName, launch.getChannelConfig())) {
            FrontEndQuery accountFrontEndQuery = playLaunchContext.getAccountFrontEndQuery();
            Restriction newAccountRestrictionForAccountQuery = applyAccountNameOrWebsiteFilterToAccountRestriction(
                    accountFrontEndQuery.getAccountRestriction().getRestriction());
            accountFrontEndQuery.setAccountRestriction(new FrontEndRestriction(newAccountRestrictionForAccountQuery));
        }
    }

    private Restriction applyEmailFilterToContactRestriction(Restriction contactRestriction) {
        Restriction emailFilter = Restriction.builder().let(BusinessEntity.Contact, InterfaceName.Email.name())
                .isNotNull().build();
        return Restriction.builder().and(contactRestriction, emailFilter).build();
    }

    private Restriction applyAccountNameOrWebsiteFilterToAccountRestriction(Restriction accountRestriction) {
        RestrictionBuilder websiteFilter = Restriction.builder()
                .let(BusinessEntity.Account, InterfaceName.Website.name()).isNotNull();
        RestrictionBuilder companyNameFilter = Restriction.builder()
                .let(BusinessEntity.Account, InterfaceName.CompanyName.name()).isNotNull();
        return Restriction.builder()
                .and(accountRestriction, Restriction.builder().or(websiteFilter, companyNameFilter).build()).build();
    }

    public PlayLaunchContext initPlayLaunchContext(Tenant tenant, CampaignLaunchInitStepConfiguration config) {
        PlayLaunchContextBuilder playLaunchContextBuilder = new PlayLaunchContextBuilder();

        CustomerSpace customerSpace = config.getCustomerSpace();
        String playName = config.getPlayName();
        String playLaunchId = config.getPlayLaunchId();

        Play play = playProxy.getPlay(customerSpace.toString(), playName);
        PlayLaunch playLaunch = playProxy.getPlayLaunch(customerSpace.toString(), playName, playLaunchId);
        List<ColumnMetadata> fieldMappingMetadata = null;

        PlayLaunchChannel playLaunchChannel = playProxy.getPlayLaunchChannelFromPlayLaunch(customerSpace.toString(),
                playName, playLaunchId);
        if (playLaunchChannel != null) {
            fieldMappingMetadata = exportFieldMetadataProxy.getExportFields(customerSpace.toString(),
                    playLaunchChannel.getId());
            playLaunch.setDestinationOrgName(playLaunchChannel.getLookupIdMap().getOrgName());
            if (fieldMappingMetadata != null) {
                log.info("For tenant= " + tenant.getName() + ", playChannelId= " + playLaunchChannel.getId()
                        + ", the columnmetadata size is=" + fieldMappingMetadata.size());
            }
        }

        long launchTimestampMillis = playLaunch.getCreated().getTime();

        RatingEngine ratingEngine = play.getRatingEngine();

        if (ratingEngine != null) {
            ratingEngine = ratingEngineProxy.getRatingEngine(customerSpace.getTenantId(), ratingEngine.getId());
        }

        MetadataSegment segment;
        if (play.getTargetSegment() != null) {
            segment = play.getTargetSegment();
        } else if (ratingEngine != null) {
            log.info(String.format(
                    "No Target segment defined for Play %s, falling back to target segment of Rating Engine %s",
                    play.getName(), ratingEngine.getId()));
            segment = play.getRatingEngine().getSegment();
        } else {
            segment = null;
        }

        if (segment == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] {
                    String.format("No Target segment defined for Campaign %s or its Model", play.getName()) });
        }

        String segmentName = segment.getName();

        Table recommendationTable = metadataProxy.getTable(tenant.getId(), playLaunch.getTableName());
        Schema schema = TableUtils.createSchema(playLaunch.getTableName(), recommendationTable);
        RatingModel publishedIteration = null;
        String modelId = null;
        String ratingId = null;
        if (ratingEngine != null) {
            publishedIteration = ratingEngine.getPublishedIteration();
            modelId = publishedIteration.getId();
            ratingId = ratingEngine.getId();
        }
        log.info(String.format("Processing segment: %s", segmentName));

        playLaunchContextBuilder //
                .customerSpace(customerSpace) //
                .tenant(tenant) //
                .launchTimestampMillis(launchTimestampMillis) //
                .playName(playName) //
                .play(play) //
                .playLaunchId(playLaunchId) //
                .playLaunch(playLaunch) //
                .ratingId(ratingId) //
                .ratingEngine(ratingEngine) //
                .publishedIterationId(modelId) //
                .publishedIteration(publishedIteration) //
                .segmentName(segmentName) //
                .segment(segment) //
                .accountFrontEndQuery(new FrontEndQuery()) //
                .contactFrontEndQuery(new FrontEndQuery()) //
                .modifiableAccountIdCollectionForContacts(new ArrayList<>()) //
                .fieldMappingMetadata(fieldMappingMetadata) //
                .counter(new Counter()) //
                .recommendationTable(recommendationTable) //
                .schema(schema);

        return playLaunchContextBuilder.build();
    }

    public PlayLaunchContext initPlayLaunchContext(Tenant tenant, DeltaCampaignLaunchInitStepConfiguration config) {
        PlayLaunchContextBuilder playLaunchContextBuilder = new PlayLaunchContextBuilder();

        CustomerSpace customerSpace = config.getCustomerSpace();
        String playName = config.getPlayName();
        String playLaunchId = config.getPlayLaunchId();

        Play play = playProxy.getPlay(customerSpace.toString(), playName);
        PlayLaunch playLaunch = playProxy.getPlayLaunch(customerSpace.toString(), playName, playLaunchId);
        List<ColumnMetadata> fieldMappingMetadata = null;

        PlayLaunchChannel playLaunchChannel = playProxy.getPlayLaunchChannelFromPlayLaunch(customerSpace.toString(),
                playName, playLaunchId);
        if (playLaunchChannel != null) {
            fieldMappingMetadata = exportFieldMetadataProxy.getExportFields(customerSpace.toString(),
                    playLaunchChannel.getId());
            playLaunch.setDestinationOrgName(playLaunchChannel.getLookupIdMap().getOrgName());
            if (fieldMappingMetadata != null) {
                log.info("For tenant= " + tenant.getName() + ", playChannelId= " + playLaunchChannel.getId()
                        + ", the columnmetadata size is=" + fieldMappingMetadata.size());
            }
        }

        long launchTimestampMillis = playLaunch.getCreated().getTime();

        RatingEngine ratingEngine = play.getRatingEngine();

        if (ratingEngine != null) {
            ratingEngine = ratingEngineProxy.getRatingEngine(customerSpace.getTenantId(), ratingEngine.getId());
        }

        MetadataSegment segment;
        if (play.getTargetSegment() != null) {
            segment = play.getTargetSegment();
        } else if (ratingEngine != null) {
            log.info(String.format(
                    "No Target segment defined for Play %s, falling back to target segment of Rating Engine %s",
                    play.getName(), ratingEngine.getId()));
            segment = play.getRatingEngine().getSegment();
        } else {
            segment = null;
        }

        if (segment == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] {
                    String.format("No Target segment defined for Campaign %s or its Model", play.getName()) });
        }

        String segmentName = segment.getName();

        Table recommendationTable = metadataProxy.getTable(tenant.getId(), playLaunch.getTableName());
        Schema schema = TableUtils.createSchema(playLaunch.getTableName(), recommendationTable);
        RatingModel publishedIteration = null;
        String modelId = null;
        String ratingId = null;
        if (ratingEngine != null) {
            publishedIteration = ratingEngine.getPublishedIteration();
            modelId = publishedIteration.getId();
            ratingId = ratingEngine.getId();
        }
        log.info(String.format("Processing segment: %s", segmentName));

        playLaunchContextBuilder //
                .customerSpace(customerSpace) //
                .tenant(tenant) //
                .launchTimestampMillis(launchTimestampMillis) //
                .playName(playName) //
                .play(play) //
                .playLaunchId(playLaunchId) //
                .playLaunch(playLaunch) //
                .ratingId(ratingId) //
                .ratingEngine(ratingEngine) //
                .publishedIterationId(modelId) //
                .publishedIteration(publishedIteration) //
                .segmentName(segmentName) //
                .segment(segment) //
                .accountFrontEndQuery(new FrontEndQuery()) //
                .contactFrontEndQuery(new FrontEndQuery()) //
                .modifiableAccountIdCollectionForContacts(new ArrayList<>()) //
                .fieldMappingMetadata(fieldMappingMetadata) //
                .counter(new Counter()) //
                .recommendationTable(recommendationTable) //
                .schema(schema);

        return playLaunchContextBuilder.build();
    }

    public void updateLaunchProgress(PlayLaunchContext playLaunchContext) {
        try {
            PlayLaunch playLaunch = playLaunchContext.getPlayLaunch();

            playProxy.updatePlayLaunchProgress(playLaunchContext.getCustomerSpace().toString(), //
                    playLaunchContext.getPlay().getName(), playLaunch.getLaunchId(),
                    playLaunch.getLaunchCompletionPercent(), playLaunch.getAccountsLaunched(),
                    playLaunch.getContactsLaunched(), playLaunch.getAccountsErrored(),
                    playLaunch.getAccountsSuppressed(), playLaunch.getContactsSuppressed(),
                    playLaunch.getContactsErrored());
        } catch (Exception e) {
            log.error("Unable to update launch progress.", e);
        }
    }

    public static class ProcessedFieldMappingMetadata {

        private List<String> accountColsRecIncluded;

        private List<String> accountColsRecNotIncludedStd;

        private List<String> accountColsRecNotIncludedNonStd;

        private List<String> contactCols;

        public List<String> getAccountColsRecIncluded() {
            return this.accountColsRecIncluded;
        }

        public void setAccountColsRecIncluded(List<String> accountColsRecIncluded) {
            this.accountColsRecIncluded = accountColsRecIncluded;
        }

        public List<String> getAccountColsRecNotIncludedStd() {
            return this.accountColsRecNotIncludedStd;
        }

        public void setAccountColsRecNotIncludedStd(List<String> accountColsRecNotIncludedStd) {
            this.accountColsRecNotIncludedStd = accountColsRecNotIncludedStd;
        }

        public List<String> getAccountColsRecNotIncludedNonStd() {
            return this.accountColsRecNotIncludedNonStd;
        }

        public void setAccountColsRecNotIncludedNonStd(List<String> accountColsRecNotIncludedNonStd) {
            this.accountColsRecNotIncludedNonStd = accountColsRecNotIncludedNonStd;
        }

        public List<String> getContactCols() {
            return this.contactCols;
        }

        public void setContactCols(List<String> contactCols) {
            this.contactCols = contactCols;
        }

    }

}
