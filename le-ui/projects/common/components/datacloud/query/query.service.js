angular.module('common.datacloud.query.service',[
])
.service('QueryStore', function($filter, $q, $timeout, QueryService, BucketRestriction) {

    this.segment = null;

    this.restriction = {
        all: [],
        any: []
    };

    this.counts = {
        accounts: {
            count: null,
            state: 'done' // 'done' | 'loading'
        },
        contacts: {
            count: null,
            state: 'done'
        }
    };

    this.setContextCount = function(context, state, count) {
        var contextCount = this.counts[context];
        if (contextCount) {
            contextCount.count = (count === undefined) ? contextCount.count : count;
            contextCount.state = (state === undefined) ? contextCount.state : state;
        }
    };

    this.getCounts = function() {
        return this.counts;
    };

    this.setRestriction = function(restriction) {
        this.restriction = restriction;
    };

    this.getRestriction = function() {
        return this.restriction;
    };

    this.setSegment = function(segment) {
        this.segment = segment;
    };

    this.getSegment = function() {
        return this.segment;
    };

    this.setupStore = function(segment) {
        var self = this;
        var deferred = $q.defer();

        for (var ctx in this.counts) {
            this.counts[ctx].state = 'loading';
        }

        this.setSegment(segment);
        if (segment !== null) {
            this.setRestriction(segment.simple_restriction);
            this.setContextCount('accounts', 'done', parseInt(this.getSegmentProperty(segment.segment_properties, 'NumAccounts')));

            deferred.resolve();
        } else {
            this.setRestriction( { all: [], any: [] } );

            this.updateContextCount('accounts').then(function() {
                deferred.resolve();
            }).catch(function(error) {
                deferred.resolve({error: error});
            });
        }

        return deferred.promise;
    };

    this.getSegmentProperty = function(properties, propertyName) {
        for (var i = 0; i < properties.length; i++) {
            var property = properties[i].metadataSegmentProperty;
            if (property.option === propertyName) {
                return property.value;
            }
        }

        return null;
    };

    this.addRestriction = function(attribute) {
        attribute.bucket = { max: 'No', min: 'No', is_null_only: false };

        var attributes = this.findAttributes(attribute.columnName);
        if (attributes.length === 0) {
            this.restriction.all.push(new BucketRestriction(attribute.columnName, attribute.bucket));
        }

        this.updateCountsDebounced();
    };

    this.removeRestriction = function(attribute) {
        var attributes = this.findAttributes(attribute.columnName);
        for (var i = 0; i < attributes.length; i++) {
            var attributeMeta = attributes[i];
            var columnName = attributeMeta.attribute.bucketRestriction.lhs.columnLookup.column_name;
            if (attribute.columnName === columnName) {
                this.restriction[attributeMeta.groupKey].splice(attributeMeta.index, 1);
                break;
            }
        }

        this.updateCountsDebounced();
    };

    this.findAttributes = function(columnName) {
        var attributes = this.findAttributesInGroup('all', columnName);
        if (attributes.length === 0) {
            attributes = this.findAttributesInGroup('any', columnName);
        }
        return attributes;
    };

    this.findAttributesInGroup = function (groupKey, columnName) {
        var group = this.restriction[groupKey];
        var results = [];

        for (var i = 0; i < group.length; i++) {
            if (group[i].bucketRestriction.lhs.columnLookup.column_name === columnName) {
                results.push({index: i, attribute: group[i], groupKey: groupKey});
            }
        }

        return results;
    };

    this.updateContextCount = function(context) {
        var self = this;
        self.setContextCount(context, 'loading')
        return this.GetCountByRestriction(context).then(function(result) {
            self.setContextCount(context, 'done', result);
        });
    };

    var debounceTime = 5000;
    var timeout = null;
    var lastUpdated = 0;
    this.updateCountsDebounced = function() {
        var self = this;
        var now = new Date().getTime();

        if (now - lastUpdated > debounceTime && !timeout) {
            this.updateContextCount('accounts').finally(function() {
                lastUpdated = now;
            });
        } else {
            $timeout.cancel(timeout);
            timeout = $timeout(function() {
                self.updateContextCount('accounts').finally(function() {
                    lastUpdated = now;
                });
            }, debounceTime - (now - lastUpdated));
        }
    };

    this.getPage = function(context, offset, maximum, query, sortBy, sortDesc) {
        return [];
    };

    this.GetCountByRestriction = function(context) {
        if (!validContext(context)) {
            var deferred = $q.defer();
            deferred.resolve({error: {errMsg:'Invalid Context: ' + context} });
            return deferred;
        }

        return QueryService.GetCountByRestriction(context, this.restriction);
    };

    this.GetCountByQuery = function(context, query) {
        query = query || {};

        if (!validContext(context)) {
            var deferred = $q.defer();
            deferred.resolve({error: {errMsg:'Invalid Context: ' + context} });
            return deferred;
        }

        return QueryService.GetCountByQuery(context);
    };

    this.GetDataByQuery = function(context) {
        if (!validContext(context)) {
            var deferred = $q.defer();
            deferred.resolve({error: {errMsg:'Invalid Context: ' + context} });
            return deferred;
        }

        return QueryService.GetDataByQuery(context);
    };

    function validContext(context) {
        return ['accounts'].indexOf(context) > -1;
    }
})
.service('QueryService', function($http, $q) {

    this.GetCountByRestriction = function(context, restriction) {
        var defer = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/' + context + '/count/restriction',
            data: restriction
        }).success(function(response) {
            defer.resolve(response);
        }).error(function(error) {
            defer.resolve({error: error});
        });

        return defer.promise;
    };

    this.GetCountByQuery = function(context, query) {
        var defer = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/' + context + '/count',
            data: query
        }).success(function(response) {
            defer.resolve(response);
        }).error(function(error) {
            defer.resolve({error: error});
        });

        return defer.promise;
    };

    this.GetDataByQuery = function (context, query) {
        var defer = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/' + context + '/data',
            data: query
        }).success(function(response) {
            defer.resolve(response);
        }).error(function(error) {
            defer.resolve({error: error});
        });

        return defer.promise;
    };
});
