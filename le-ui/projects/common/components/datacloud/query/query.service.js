angular.module('common.datacloud.queryservice',[
])
.service('QueryStore', function($filter, $q, QueryService) {

    this.segment = null;

    this.restriction = {
        all: [],
        any: []
    };

    this.counts = {
        accounts: null,
        contacts: null
    };

    this.getRestriction = function() {
        return this.restriction;
    };

    this.setRestriction = function(restriction) {
        restriction = restriction || { all: [], any: [] };
        this.restriction = restriction;
    };

    this.setSegmentAndRestriction = function(segment) {
        this.segment = segment;
        this.setRestriction(segment !== null ? segment.simple_restriction : null);
    };

    this.addRestriction = function(attribute) {
        // append to all (default)
    };

    this.removeRestriction = function(attribute) {
        // search and remove from all or any
    };

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
