angular.module('mainApp.appCommon.utilities.SegmentsUtility', [])
.service('SegmentsUtility', function () {

    this.PropertyBag = function (name, segment) {
        var property = '';
        if(segment && segment.segment_properties) {
            var segment_properties = segment.segment_properties;
            for(var i in segment_properties) {
                var property = segment_properties[i],
                    metadata = property.metadataSegmentProperty || {};
                if(metadata && metadata.option && metadata.option === name) {
                    return metadata.value || '';
                }
            }
        }
    };

});