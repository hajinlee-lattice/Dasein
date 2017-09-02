angular
.module('lp.models.segments')
.service('SegmentStore', function($q, SegmentService) {
    var SegmentStore = this;

    this.segments = [];

    this.setSegments = function(segments) {
        this.segments = segments;
    }

    this.getSegments = function() {
        return this.segments;
    }

    this.getSegmentByName = function(segmentName) {
        var deferred = $q.defer(),
            found = false;

        for (var i = 0; i < this.segments.length; i++) {
            var segment = this.segments[i];

            if (segment.name === segmentName) {
                deferred.resolve(segment);
                found = true;

                break;
            }
        }

        if (!found) {
            SegmentService.GetSegmentByName(segmentName).then(function(result) {
                deferred.resolve(result ? result : null);
            });
        }

        return deferred.promise;
    }

    this.CreateOrUpdateSegment = function(segment, restriction) {
        var ts = new Date().getTime();

        if (!segment) {
            segment = {
                'name': 'segment' + ts,
                'display_name': 'segment' + ts,
                'account_restriction': restriction,
                'page_filter': {
                    'row_offset': 0,
                    'num_rows': 10
                }
            };
        } else {
            segment = {
                'name': segment.name,
                'display_name': segment.display_name,
                'account_restriction': restriction || segment.account_restriction,
                'page_filter': {
                    'row_offset': 0,
                    'num_rows': 10
                }
            };
        }

        this.sanitizeSegment(segment);

        return SegmentService.CreateOrUpdateSegment(segment);
    }

    this.sanitizeSegment = function(segment) {
        var restriction = segment.account_restriction.restriction;

        this.sanitizeSegmentRestriction([ restriction ]);

        return segment;
    }

    this.sanitizeSegmentRestriction = function(tree) {
        tree.forEach(function(branch) {
            if (branch && typeof branch.labelGlyph != undefined) {
                delete branch.labelGlyph;
            }

            if (branch && typeof branch.collapsed != undefined) {
                delete branch.collapsed;
            }

            if (branch && branch.logicalRestriction) {
                SegmentStore.sanitizeSegmentRestriction(branch.logicalRestriction.restrictions);
            }
        });
    }
})
.service('SegmentService', function($http, $q, $state) {
    this.GetSegments = function() {
        var deferred = $q.defer(),
            result,
            url = '/pls/datacollection/segments';

        $http({
            method: 'GET',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                result = response.data;
                deferred.resolve(result);

            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.reject(errorMsg);
            }
        );
        return deferred.promise;
    }

    this.GetSegmentByName = function(name) {
        var deferred = $q.defer(),
            result,
            url = '/pls/datacollection/segments/' + name;

        $http({
            method: 'GET',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                result = response.data;
                deferred.resolve(result);

            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.reject(errorMsg);
            }
        );
        return deferred.promise;
    }

    this.CreateOrUpdateSegment = function(segment) {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/datacollection/segments',
            data: segment,
            headers: {
                'Content-Type': 'application/json'
                // 'ErrorDisplayMethod': 'none' // segment:demo:fixme
            }
        }).then(
            function onSuccess(response) {
                var result = {
                    data: response.data,
                    success: true
                };
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }
                var result = {
                    data: response.data,
                    errorMsg: (response.data.errorMsg ? response.data.errorMsg : 'unspecified error'),
                    success: false
                };
                deferred.resolve(result);
            }
        )

        return deferred.promise;
    }

    this.DeleteSegment = function(segmentName) {
        var deferred = $q.defer(),
            result = {},
            url = '/pls/datacollection/segments/' + segmentName;

        $http({
            method: 'DELETE',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                result = {
                    data: response.data,
                    success: true
                };
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.reject(errorMsg);
            }
        );

        return deferred.promise;
    }


});