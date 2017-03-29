angular.module('common.datacloud.queryservice',[
])
.service('QueryStore', function($filter, $q, QueryService) {

    function BucketRestriction(columnName, bucket) {
        this.lhs = {
            columnLookup: {
                column_name: columnName
            }
        };
        this.range = bucket;
    }

    this.segment = null;

    this.restriction = {
        all: [],
        any: []
    };

    this.counts = {
        accounts: null,
        contacts: null
    };

    /* restriction related */
    this.getRestriction = function() {
        return this.restriction;
    };
    peakRestriction = this.getRestriction.bind(this);

    this.setRestriction = function(restriction) {
        restriction = restriction || { all: [], any: [] };
        this.restriction = angular.copy(restriction);
    };

    this.setSegment = function(segment) {
        this.segment = segment;
        this.setRestriction(segment !== null ? segment.simple_restriction : null);
    };

    this.getSegment = function() {
        return this.segment;
    };

    this.addRestriction = function(attribute) {
        attribute.bucket = { max: 'No', min: 'No', is_null_only: false };

        var attributes = this.findAttributes(attribute.columnName);
        if (attributes.length === 0) {
            this.restriction.all.push({ bucketRestriction: new BucketRestriction(attribute.columnName, attribute.bucket) });
        }
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

    /* query related */
    this.getPage = function(context, offset, maximum, query, sortBy, sortDesc) {
        return [];
    };

    this.getCount = function(context) {
        return this.GetCountByRestriction(context, this.restriction);
    };

    this.GetCountByRestriction = function(context) {
        if (!validContext(context)) {
            return $q.defer().resolve({error: {errMsg:'Invalid Context: ' + context} });
        }

        return 0; // return QueryService.GetCountByRestriction(context, this.restriction);
    };

    this.GetCountByQuery = function(context, query) {
        query = query || {};

        if (!validContext(context)) {
            return $q.defer().resolve({error: {errMsg:'Invalid Context: ' + context} });
        }

        return QueryService.GetCountByQuery(context);
    };

    this.GetDataByQuery = function(context) {
        if (!validContext(context)) {
            return $q.defer().resolve({error: {errMsg:'Invalid Context: ' + context} });
        }

        return QueryService.GetDataByQuery(context);
    };

    function validContext(context) {
        return ['accounts', 'contacts'].indexOf(context) > -1;
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
