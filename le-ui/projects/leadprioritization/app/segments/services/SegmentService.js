angular
.module('lp.segments.segments')
.service('SegmentStore', function($q, SegmentService) {
    var SegmentStore = this;

    this.segments = [];

    this.setSegments = function(segments) {
        this.segments = segments;
    }

    this.getSegments = function() {
        console.log(this.segments);
        return this.segments;
    }

    this.flattenSegmentRestrictions = function(segment) {
        restrictions = [];
        if (segment.account_restriction != null && 
            segment.account_restriction.restriction !== null  && 
            segment.account_restriction.restriction.logicalRestriction && 
            segment.contact_restriction != null && 
            segment.contact_restriction.restriction !== null &&
            segment.contact_restriction.restriction.logicalRestriction) {
            segment.account_restriction.restriction.logicalRestriction.restrictions.forEach(function(restriction) {
                SegmentStore.flattenRestriction(restriction, restrictions);
            });
            segment.contact_restriction.restriction.logicalRestriction.restrictions.forEach(function(restriction) {
                SegmentStore.flattenRestriction(restriction, restrictions);
            });
        }
        // else {
        //     console.log('Segment restrictions null ');
        //     console.log(segment);
        //     console.log('============================');
        // }
        return restrictions;
    }

    this.flattenRestriction = function(restriction, array) {
        if (restriction.bucketRestriction) {
            array.push(restriction);
        } else if (restriction.logicalRestriction) {
            restriction.logicalRestriction.restrictions.forEach(function(restriction) {
                SegmentStore.flattenRestriction(restriction, array);
            })
        }
    }

    this.getTopNAttributes = function(segment, n){
        var restrictions = SegmentStore.flattenSegmentRestrictions(segment);

        if (n > restrictions.length) {
          return restrictions;
        }
        var minIndex = 0, 
            i;

        for (i = n; i < restrictions.length; i++){
            minIndex = 0;
            for (var j = 0; j < n; j++){
                if(restrictions[minIndex].bucketRestriction.bkt.Cnt > restrictions[j].bucketRestriction.bkt.Cnt){
                    minIndex = j;
                    restrictions[minIndex] = restrictions[j];
                }
            }    
            if (restrictions[minIndex].bucketRestriction.bkt.Cnt < restrictions[i].bucketRestriction.bkt.Cnt){
                SegmentStore.swap(restrictions, minIndex, i);
            }
        }

        var result = restrictions.splice(0, n); // unsorted list of top n attributes by volume
        return result;
    }

    this.sortAttributesByCnt = function(restrictions) {
        var counts = restrictions.map(function (restriction, idx) {
            return {index: idx, count: restriction.bucketRestriction.bkt.Cnt };
        });
        counts.sort(function (a, b) {
            return b.count - a.count;
        });
        restrictions = counts.map(function (restriction) {
            return restrictions[restriction.index];
        });
        return restrictions;
    }

    this.swap = function(array, i, j) {
        var temp = array[i];
        array[i] = array[j];
        array[j] = temp;
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

        // remove Advanced Query Builder-specific temp properties
        this.sanitizeSegment(segment);

        return SegmentService.CreateOrUpdateSegment(segment);
    }

    this.sanitizeSegment = function(segment) {
        var aRestriction = segment.account_restriction
            ? segment.account_restriction.restriction
            : {};

        var cRestriction = segment.contact_restriction
            ? segment.contact_restriction.restriction
            : {};

        this.sanitizeSegmentRestriction([ aRestriction ]);
        this.sanitizeSegmentRestriction([ cRestriction ]);

        return segment;
    }

    this.sanitizeRuleBuckets = function(rule, keepEmptyBuckets) {
        var map = rule.ratingRule.bucketToRuleMap,
            prune = [];

        if (!keepEmptyBuckets) {
            Object.keys(map).forEach(function(bucketName) {
                var account = map[bucketName].account_restriction.logicalRestriction.restrictions;
                var contact = map[bucketName].contact_restriction.logicalRestriction.restrictions;

                if (account.length + contact.length == 0) {
                    prune.push(bucketName);
                }
            });

            for (var i = prune.length - 1; i >= 0; i--) {
                delete map[prune[i]];
            }
        }

        Object.keys(map).forEach(function(bucketName) {
            var bucket = map[bucketName];

            if (bucket) {
                if (!keepEmptyBuckets) {
                    SegmentStore.removeEmptyBuckets([ bucket.account_restriction ]);
                    SegmentStore.removeEmptyBuckets([ bucket.contact_restriction ]);
                }

                SegmentStore.sanitizeSegmentRestriction([ bucket.account_restriction ]);
                SegmentStore.sanitizeSegmentRestriction([ bucket.contact_restriction ]);
            }
        });

        return rule;
    }

    this.sanitizeSegmentRestriction = function(tree) {
        tree.forEach(function(branch) {
            if (branch && typeof branch.labelGlyph !== undefined) {
                delete branch.labelGlyph;
            }

            if (branch && typeof branch.collapsed !== undefined) {
                delete branch.collapsed;
            }

            if (branch && branch.logicalRestriction) {
                SegmentStore.sanitizeSegmentRestriction(branch.logicalRestriction.restrictions);
            }
        });

        return tree;
    }

    this.removeEmptyBuckets = function(tree) {
        for (var branch, i = tree.length - 1; i >= 0; i--) {
            branch = tree[i];

            if (branch && branch.bucketRestriction && (!branch.bucketRestriction.bkt || !branch.bucketRestriction.bkt.Id)) {
                tree.splice(i, 1);
            }

            if (branch && branch.logicalRestriction) {
                SegmentStore.removeEmptyBuckets(branch.logicalRestriction.restrictions);
            }
        }
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

    this.GetSegmentExports = function() {
        var deferred = $q.defer(),
            result,
            url = '/pls/datacollection/segments/export';

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

    this.GetSegmentExportByExportId = function(exportID) {
        var deferred = $q.defer(),
            result,
            url = '/pls/datacollection/segments/export/' + exportID;

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

    this.CreateOrUpdateSegmentExport = function(segment) {
        var deferred = $q.defer(),
            result = {}, 
            url = '/pls/datacollection/segments/export';

        $http({
            method: 'POST',
            url: url,
            data: segment,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                result = {
                    data: response.data,
                    success: true
                }
                deferred.resolve(result);

            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }
                result = {
                    data: response.data,
                    errorMsg: (response.data.errorMsg ? response.data.errorMsg : 'unspecified error'),
                    success: false
                };
                deferred.resolve(result);
                // var errorMsg = response.data.errorMsg || 'unspecified error';
                // deferred.resolve(errorMsg);
            }
        );
        return deferred.promise;
    }

    this.DeleteExpiredSegmentExports = function() {
        var deferred = $q.defer(),
            result = {},
            url = '/pls/datacollection/segments/export/cleanup';

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

    this.DownloadExportedSegment = function(id) {
        var deferred = $q.defer(),
            result,
            url = '/pls/datacollection/segments/export/' + id + '/download';

        $http({
            method: 'GET',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                deferred.resolve(response);

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