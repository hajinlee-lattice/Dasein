package com.latticeengines.domain.exposed.metadata;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.db.PropertyBag;

public class MetadataSegmentPropertyBag extends PropertyBag<MetadataSegmentProperty, MetadataSegmentPropertyName> {

    @SuppressWarnings("unchecked")
    public MetadataSegmentPropertyBag(List<MetadataSegmentProperty> segmentProperties) {
        super(List.class.cast(segmentProperties));
    }

    @SuppressWarnings("unchecked")
    public MetadataSegmentPropertyBag() {
        super(List.class.cast(new ArrayList<MetadataSegmentProperty>()));
    }

    @JsonProperty
    public List<MetadataSegmentProperty> getBag() {
        return this.bag;
    }

    public void setBag(List<MetadataSegmentProperty> segmentProperties) {
        this.bag = segmentProperties;
    }

    public void setMetadataSegment(MetadataSegment metadataSegment) {
        for (MetadataSegmentProperty metadataSegmentProperty : this.getBag()) {
            metadataSegmentProperty.setOwner(metadataSegment);
        }
    }

    public void setProvenanceProperty(MetadataSegmentPropertyName metadataSegmentPropertyName, Object value) {
        this.set(metadataSegmentPropertyName, value);
    }

}
