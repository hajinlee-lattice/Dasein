package com.latticeengines.cdl.workflow.steps.play;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchSparkContext;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;

public class PlayLaunchContext {

    private CustomerSpace customerSpace;

    private Tenant tenant;

    private String playName;

    private String playLaunchId;

    private PlayLaunch playLaunch;

    private Play play;

    private long launchTimestampMillis;

    private RatingEngine ratingEngine;

    private MetadataSegment segment;

    private String segmentName;

    private String modelId;

    private String ratingId;

    private RatingModel publishedIteration;

    private FrontEndQuery accountFrontEndQuery;

    private FrontEndQuery contactFrontEndQuery;

    private List<Object> modifiableAccountIdCollectionForContacts;

    private List<ColumnMetadata> fieldMappingMetadata;

    private Counter counter;

    private Table recommendationTable;

    private Schema schema;

    public PlayLaunchContext(CustomerSpace customerSpace, Tenant tenant, String playName, String playLaunchId,
            PlayLaunch playLaunch, Play play, long launchTimestampMillis, RatingEngine ratingEngine,
            MetadataSegment segment, String segmentName, String modelId, String ratingId,
            RatingModel publishedIteration, FrontEndQuery accountFrontEndQuery, FrontEndQuery contactFrontEndQuery,
            List<Object> modifiableAccountIdCollectionForContacts, List<ColumnMetadata> fieldMappingMetadata,
            Counter counter, Table recommendationTable, Schema schema) {
        super();
        this.customerSpace = customerSpace;
        this.tenant = tenant;
        this.playName = playName;
        this.playLaunchId = playLaunchId;
        this.playLaunch = playLaunch;
        this.play = play;
        this.launchTimestampMillis = launchTimestampMillis;
        this.ratingEngine = ratingEngine;
        this.segment = segment;
        this.segmentName = segmentName;
        this.modelId = modelId;
        this.ratingId = ratingId;
        this.publishedIteration = publishedIteration;
        this.accountFrontEndQuery = accountFrontEndQuery;
        this.contactFrontEndQuery = contactFrontEndQuery;
        this.modifiableAccountIdCollectionForContacts = modifiableAccountIdCollectionForContacts;
        this.fieldMappingMetadata = fieldMappingMetadata;
        this.counter = counter;
        this.recommendationTable = recommendationTable;
        this.schema = schema;
    }

    public PlayLaunchSparkContext toPlayLaunchSparkContext() {
        return new PlayLaunchSparkContext(this.tenant, this.playName, this.playLaunchId, this.playLaunch, this.play,
                this.ratingEngine, this.segment, this.launchTimestampMillis, this.ratingId, this.publishedIteration);
    }

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public String getPlayName() {
        return playName;
    }

    public String getPlayLaunchId() {
        return playLaunchId;
    }

    public PlayLaunch getPlayLaunch() {
        return playLaunch;
    }

    public Play getPlay() {
        return play;
    }

    public long getLaunchTimestampMillis() {
        return launchTimestampMillis;
    }

    public RatingEngine getRatingEngine() {
        return ratingEngine;
    }

    public MetadataSegment getSegment() {
        return segment;
    }

    public String getSegmentName() {
        return segmentName;
    }

    public RatingModel getPublishedIteration() {
        return publishedIteration;
    }

    public String getModelId() {
        return modelId;
    }

    public String getRatingId() {
        return ratingId;
    }

    public FrontEndQuery getAccountFrontEndQuery() {
        return accountFrontEndQuery;
    }

    public FrontEndQuery getContactFrontEndQuery() {
        return contactFrontEndQuery;
    }

    public FrontEndQuery getClonedAccountFrontEndQuery() {
        return accountFrontEndQuery == null ? null : JsonUtils.clone(accountFrontEndQuery);
    }

    public FrontEndQuery getClonedContactFrontEndQuery() {
        return contactFrontEndQuery == null ? null : JsonUtils.clone(contactFrontEndQuery);
    }

    public List<Object> getModifiableAccountIdCollectionForContacts() {
        return modifiableAccountIdCollectionForContacts;
    }

    public List<ColumnMetadata> getFieldMappingMetadata() {
        return fieldMappingMetadata;
    }

    public Counter getCounter() {
        return counter;
    }

    public Table getRecommendationTable() {
        return recommendationTable;
    }

    public Schema getSchema() {
        return schema;
    }

    public static class PlayLaunchContextBuilder {
        private CustomerSpace customerSpace;

        private Tenant tenant;

        private String playName;

        private String playLaunchId;

        private PlayLaunch playLaunch;

        private Play play;

        private long launchTimestampMillis;

        private RatingEngine ratingEngine;

        private MetadataSegment segment;

        private String segmentName;

        private String publishedIterationId;

        private String ratingId;

        private RatingModel publishedIteration;

        private FrontEndQuery accountFrontEndQuery;

        private FrontEndQuery contactFrontEndQuery;

        private List<Object> modifiableAccountIdCollectionForContacts;

        private List<ColumnMetadata> fieldMappingMetadata;

        private Counter counter;

        private Table recommendationTable;

        private Schema schema;

        public PlayLaunchContextBuilder() {
        }

        public PlayLaunchContextBuilder customerSpace(CustomerSpace customerSpace) {
            this.customerSpace = customerSpace;
            return this;
        }

        public PlayLaunchContextBuilder tenant(Tenant tenant) {
            this.tenant = tenant;
            return this;
        }

        public PlayLaunchContextBuilder playName(String playName) {
            this.playName = playName;
            return this;
        }

        public PlayLaunchContextBuilder playLaunchId(String playLaunchId) {
            this.playLaunchId = playLaunchId;
            return this;
        }

        public PlayLaunchContextBuilder playLaunch(PlayLaunch playLaunch) {
            this.playLaunch = playLaunch;
            return this;
        }

        public PlayLaunchContextBuilder play(Play play) {
            this.play = play;
            return this;
        }

        public PlayLaunchContextBuilder launchTimestampMillis(long launchTimestampMillis) {
            this.launchTimestampMillis = launchTimestampMillis;
            return this;
        }

        public PlayLaunchContextBuilder ratingEngine(RatingEngine ratingEngine) {
            this.ratingEngine = ratingEngine;
            return this;
        }

        public PlayLaunchContextBuilder segment(MetadataSegment segment) {
            this.segment = segment;
            return this;
        }

        public PlayLaunchContextBuilder segmentName(String segmentName) {
            this.segmentName = segmentName;
            return this;
        }

        public PlayLaunchContextBuilder publishedIterationId(String publishedIterationId) {
            this.publishedIterationId = publishedIterationId;
            return this;
        }

        public PlayLaunchContextBuilder ratingId(String ratingId) {
            this.ratingId = ratingId;
            return this;
        }

        public PlayLaunchContextBuilder publishedIteration(RatingModel publishedIteration) {
            this.publishedIteration = publishedIteration;
            return this;
        }

        public PlayLaunchContextBuilder accountFrontEndQuery(FrontEndQuery accountFrontEndQuery) {
            this.accountFrontEndQuery = accountFrontEndQuery;
            return this;
        }

        public PlayLaunchContextBuilder contactFrontEndQuery(FrontEndQuery contactFrontEndQuery) {
            this.contactFrontEndQuery = contactFrontEndQuery;
            return this;
        }

        public PlayLaunchContextBuilder modifiableAccountIdCollectionForContacts(
                List<Object> modifiableAccountIdCollectionForContacts) {
            this.modifiableAccountIdCollectionForContacts = modifiableAccountIdCollectionForContacts;
            return this;
        }

        public PlayLaunchContextBuilder fieldMappingMetadata(List<ColumnMetadata> fieldMappingMetadata) {
            this.fieldMappingMetadata = fieldMappingMetadata;
            return this;
        }

        public PlayLaunchContextBuilder counter(Counter counter) {
            this.counter = counter;
            return this;
        }

        public PlayLaunchContextBuilder recommendationTable(Table recommendationTable) {
            this.recommendationTable = recommendationTable;
            return this;
        }

        public PlayLaunchContextBuilder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public PlayLaunchContext build() {
            return new PlayLaunchContext(customerSpace, tenant, playName, playLaunchId, playLaunch, play,
                    launchTimestampMillis, ratingEngine, segment, segmentName, publishedIterationId, ratingId,
                    publishedIteration, accountFrontEndQuery, contactFrontEndQuery,
                    modifiableAccountIdCollectionForContacts, fieldMappingMetadata, counter, recommendationTable,
                    schema);
        }
    }

    public static class Counter {
        private AtomicLong accountLaunched;
        private AtomicLong contactLaunched;
        private AtomicLong accountErrored;
        private AtomicLong contactErrored;

        public Counter() {
            accountLaunched = new AtomicLong();
            contactLaunched = new AtomicLong();
            accountErrored = new AtomicLong();
            contactErrored = new AtomicLong();
        }

        public AtomicLong getAccountLaunched() {
            return accountLaunched;
        }

        public AtomicLong getContactLaunched() {
            return contactLaunched;
        }

        public AtomicLong getAccountErrored() {
            return accountErrored;
        }

        public AtomicLong getContactErrored() {
            return contactErrored;
        }
    }
}
