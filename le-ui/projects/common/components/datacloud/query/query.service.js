angular.module('common.datacloud.query.service',[
])
.service('QueryStore', function($filter, $q, QueryService, BucketRestriction, QueryServiceStub) {

    angular.extend(this, QueryServiceStub);

    this.validContexts = ['accounts', 'contacts'];
    this.segment = null;
    this.restriction = {
        all: [],
        any: []
    };
    this.counts = {
        accounts: {
            value: 0,
            loading: false
        },
        contacts: {
            value: 0,
            loading: false
        }
    };

    this.getCounts = function() {
        return this.counts;
    };

    this.setContextCount = function(context, loading, value) {
        var contextCount = this.getCounts()[context];
        if (contextCount) {
            if (typeof value  !== 'undefined') {
                contextCount.value = value;
            }
            if (typeof loading !== 'undefined') {
                contextCount.loading = loading;
            }
        }
    };

    this.setRestriction = function(restriction) {
        this.restriction = restriction;
    };

    this.getRestriction = function() {
        return this.restriction;
    };

    this.updateRestriction = function(restriction) {
        this.restriction.all = restriction.all;
        this.restriction.any = restriction.any;

        this.initUiStates(this.getRestriction());
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

        this.setSegment(segment);
        if (segment !== null) {
            this.setRestriction(segment.simple_restriction || { all: [], any: [] });
            deferred.resolve();
        } else {
            this.setRestriction( { all: [], any: [] } );
        }

        this.initUiStates(this.getRestriction());

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

    var tickTock = true;
    this.addRestriction = function(attribute) {
        attribute.bucket = (tickTock = !tickTock) ? { max: 'No', min: 'No', is_null_only: false } : { max: 'Yes', min: 'Yes', is_null_only: false };

        var attributes = this.findAttributes(attribute.columnName);
        var found = false;
        for (var i = 0; i < attributes.length; i++) {
            var attributeMeta = attributes[i];
            if (BucketRestriction.isEqualBucket(attribute, attributeMeta.attribute)) {
                found = true;
                break;
            }
        }

        if (!found) {
            this.restriction.all.push(new BucketRestriction(attribute.columnName, attribute.bucket));
            this.updateUiState(attribute.columnName, 1, this.restriction.all.length + this.restriction.any.length);
        }
    };

    this.removeRestriction = function(attribute) {
        var attributes = this.findAttributes(attribute.columnName);
        for (var i = 0; i < attributes.length; i++) {
            var attributeMeta = attributes[i];
            var columnName = attributeMeta.attribute.bucketRestriction.lhs.columnLookup.column_name;
            if (attribute.columnName === columnName) {
                this.restriction[attributeMeta.groupKey].splice(attributeMeta.index, 1);
                this.updateUiState(attribute.columnName, -1, this.restriction.all.length + this.restriction.any.length);
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

    this.GetCountByRestriction = function(context) {
        if (!this.isValidContext(context)) {
            var deferred = $q.defer();
            deferred.resolve({error: {errMsg:'Invalid Context: ' + context} });
            return deferred;
        }

        return QueryService.GetCountByRestriction(context, this.restriction);
    };

    this.GetCountByQuery = function(context, query) {
        query.restriction = this.getRestriction();

        if (!this.isValidContext(context)) {
            var deferred = $q.defer();
            deferred.resolve({error: {errMsg:'Invalid Context: ' + context} });
            return deferred.promise;
        }

        return QueryService.GetCountByQuery(context, query);
    };

    this.GetDataByQuery = function(context, query) {
        query.restriction = this.getRestriction();

        if (!this.isValidContext(context)) {
            var deferred = $q.defer();
            deferred.resolve({error: {errMsg:'Invalid Context: ' + context} });
            return deferred.promise;
        }

        return QueryService.GetDataByQuery(context, query);
    };

    this.isValidContext = function(context) {
        return this.validContexts.indexOf(context) > -1;
    };

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
})
// stubbed for demo
.service('QueryServiceStub', function($http, $timeout, BucketRestriction) {

    var uiStates = [];
    var defaultState = { "name": "default", "bitmask": 0, "nodes":{}, "counts":{"accounts":null, "contacts":null} }
    var undefinedState = { "name": "default", "bitmask": -1, "nodes":{}, "counts":{"accounts":0, "contacts":0} }
    var self = this;

    this.columns = {
        accounts: [],
        contacts: []
    };

    this.records = {
        accounts: [],
        contacts: []
    };

    this.uiState = defaultState;
    this.uiStateMap = {};

    this.setUiState = function(uiState) {
        this.uiState = uiState;
        this.updateCount();
    };

    this.loadData = function(context) { // combine accounts and contacts if file is small enough
        var self = this;
        self.setContextCount(context, true);

        return $http({
            method: 'GET',
            url: 'assets/resources/stub/'+context+'.json',
        }).then(function(result) {
            var data = result.data || {}
            self.columns[context] = data.columns || [];
            self.records[context] = data.records || [];

            if (self.uiState === defaultState) {
                self.uiState.counts[context] = data.count;
            }
        });
    };

    var timeout = {
        accounts: null,
        contacts: null
    };

    this.updateCount = function() {
        var self = this;
        var delay = 500;

        for (var i = 0; i < this.validContexts.length; i++){
            var context = this.validContexts[i];
            this.setContextCount(context, true);

            if (timeout[context]) {
                $timeout.cancel(timeout[context]);
            }

            timeout[context] = $timeout((function(x) {
                return function () {
                    self.setContextCount(x, false, self.uiState.counts[x]);
                }
            })(context), delay + delay * Math.random());
        }
    };

    this.updateUiStateMapAndGetState = function(columnName, step, totalLen) {
        var uiStateFound = null;
        for (var j = 0; j < uiStates.length; j++) {
            var uiState = uiStates[j];
            if (typeof uiState.nodes[columnName] !== 'undefined') {
                this.uiStateMap[uiState.name] += step;
            }
            if (this.uiStateMap[uiState.name] === Object.keys(uiState.nodes).length &&
                this.uiStateMap[uiState.name] === totalLen) {
                uiStateFound = uiState;
            }
        }
        return uiStateFound;
    };

    this.initUiStates = function(restriction) {
        for (var i = 0; i < uiStates.length; i++) {
            this.uiStateMap[uiStates[i].name] = 0;
        }

        var totalLen = restriction.all.length + restriction.any.length;
        if (totalLen === 0) {
            this.setUiState(defaultState);
            return;
        }

        var uiStateFound = null;
        for (var groupKey in restriction) {
            var group = restriction[groupKey];
            for (var i = 0; i < group.length; i++) {
                var columnName = BucketRestriction.getColumnFromBucket(group[i]);

                uiStateFound = this.updateUiStateMapAndGetState(columnName, 1, totalLen);
            }
        }

        this.setUiState(uiStateFound || undefinedState);
    };

    this.updateUiState = function(columnName, step, totalLen) { // step +1 | -1
        var uiStateFound = this.updateUiStateMapAndGetState(columnName, step, totalLen);

        if (uiStateFound === null && totalLen === 0) {
            uiStateFound = defaultState;
        }
        this.setUiState(uiStateFound || undefinedState);
    };

    this.getRecordsForUiState = function(context) {
        if (!this.isValidContext(context)) {
            return [];
        }

        var bitmask = this.uiState.bitmask;

        return this.records[context].filter(function(record) {
            return (record.uiState & bitmask) === bitmask;
        });
    };

});
