angular.module('common.datacloud.query.service',[
])
.service('QueryStore', function($filter, $q, QueryService, BucketRestriction, QueryServiceStub) {

    angular.extend(this, QueryServiceStub);

    this.validResourceTypes = ['accounts', 'contacts'];
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

    this.setResourceTypeCount = function(resourceType, loading, value) {
        var resourceTypeCount = this.getCounts()[resourceType];
        if (resourceTypeCount) {
            if (typeof value  !== 'undefined') {
                resourceTypeCount.value = value;
            }
            if (typeof loading !== 'undefined') {
                resourceTypeCount.loading = loading;
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

    this.addRestriction = function(attribute) {
        attribute.range = attribute.range || { max: 'No', min: 'No', is_null_only: false };
        attribute.resourceType = attribute.resourceType || 'BucketedAccountMaster';

        var attributesFound = this.findAttributes(attribute.columnName);
        var attributes = attributesFound.attributes;
        var groupKey = attributesFound.groupKey;

        var found = false;
        for (var i = 0; i < attributes.length; i++) {
            var attributeMeta = attributes[i];
            if (BucketRestriction.isEqualRange(attribute.range, BucketRestriction.getRange(attributeMeta.bucketRestriction))) {
                found = true;
                break;
            }
        }

        if (!found) {
            groupKey = groupKey || 'all';
            this.restriction[groupKey].push({
                bucketRestriction: new BucketRestriction(attribute.columnName, attribute.resourceType, attribute.range)
            });
            this.updateUiState(attribute.columnName, 1, this.restriction.all.length + this.restriction.any.length);
        }
    };

    this.removeRestriction = function(attribute) {
        attribute.range = attribute.range || { max: 'No', min: 'No', is_null_only: false };

        var attributesFound = this.findAttributes(attribute.columnName);
        var attributes = attributesFound.attributes;
        var groupKey = attributesFound.groupKey;

        for (var i = 0; i < attributes.length; i++) {
            var attributeMeta = attributes[i];
            var columnName = BucketRestriction.getColumnName(attributeMeta.bucketRestriction);
            if (attribute.columnName === columnName &&
                BucketRestriction.isEqualRange(attribute.range, BucketRestriction.getRange(attributeMeta.bucketRestriction))) {
                this.restriction[groupKey].splice(attributeMeta.index, 1);
                this.updateUiState(attribute.columnName, -1, this.restriction.all.length + this.restriction.any.length);
                break;
            }
        }
    };

    this.findAttributes = function(columnName) {
        var groupKey = null;
        var attributes = [];
        for (var group in this.restriction) {
            attributes = this.findAttributesInGroup(group, columnName);
            if (attributes.length > 0) {
                groupKey = group;
                break;
            }
        }
        return { groupKey: groupKey, attributes: attributes };
    };

    this.findAttributesInGroup = function(groupKey, columnName) {
        var group = this.restriction[groupKey];
        var results = [];

        for (var i = 0; i < group.length; i++) {
            if (group[i].bucketRestriction.lhs.columnLookup.column_name === columnName) {
                results.push({index: i, bucketRestriction: group[i].bucketRestriction });
            }
        }

        return results;
    };

    this.GetCountByRestriction = function(resourceType) {
        if (!this.isValidResourceType(resourceType)) {
            var deferred = $q.defer();
            deferred.resolve({error: {errMsg:'Invalid resourceType: ' + resourceType} });
            return deferred;
        }

        return QueryService.GetCountByRestriction(resourceType, this.restriction);
    };

    this.GetCountByQuery = function(resourceType, query) {
        query.restriction = this.getRestriction();

        if (!this.isValidResourceType(resourceType)) {
            var deferred = $q.defer();
            deferred.resolve({error: {errMsg:'Invalid resourceType: ' + resourceType} });
            return deferred.promise;
        }

        return QueryService.GetCountByQuery(resourceType, query);
    };

    this.GetDataByQuery = function(resourceType, query) {
        query.restriction = this.getRestriction();

        if (!this.isValidResourceType(resourceType)) {
            var deferred = $q.defer();
            deferred.resolve({error: {errMsg:'Invalid resourceType: ' + resourceType} });
            return deferred.promise;
        }

        return QueryService.GetDataByQuery(resourceType, query);
    };

    this.isValidResourceType = function(resourceType) {
        return this.validResourceTypes.indexOf(resourceType) > -1;
    };

})
.service('QueryService', function($http, $q) {

    this.GetCountByRestriction = function(resourceType, restriction) {
        var defer = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/' + resourceType + '/count/restriction',
            data: restriction
        }).success(function(response) {
            defer.resolve(response);
        }).error(function(error) {
            defer.resolve({error: error});
        });

        return defer.promise;
    };

    this.GetCountByQuery = function(resourceType, query) {
        var defer = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/' + resourceType + '/count',
            data: query
        }).success(function(response) {
            defer.resolve(response);
        }).error(function(error) {
            defer.resolve({error: error});
        });

        return defer.promise;
    };

    this.GetDataByQuery = function(resourceType, query) {
        var defer = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/' + resourceType + '/data',
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

    var uiStates = [{"name":"a","bitmask":256,"counts":{"accounts":39,"contacts":204},"nodes":{"Demo_Title":true}},{"name":"b","bitmask":128,"counts":{"accounts":33,"contacts":62},"nodes":{"Demo_Department":true,"Demo_Title":true}},{"name":"c","bitmask":64,"counts":{"accounts":33,"contacts":61},"nodes":{"Demo_Is_With_Company":true,"Demo_Department":true,"Demo_Title":true}},{"name":"d","bitmask":32,"counts":{"accounts":6,"contacts":16},"nodes":{"Demo_Has_Purchased":true,"Demo_Is_With_Company":true,"Demo_Department":true,"Demo_Title":true}},{"name":"e","bitmask":16,"counts":{"accounts":2,"contacts":4},"nodes":{"A":true,"Demo_Has_Purchased":true,"Demo_Is_With_Company":true,"Demo_Department":true,"Demo_Title":true}},{"name":"f","bitmask":8,"counts":{"accounts":1,"contacts":3},"nodes":{"Demo_Last_Quarter_Spending_Trends_(%)":true,"Lattice_Rating":true,"Demo_Has_Purchased":true,"Demo_Is_With_Company":true,"Demo_Department":true,"Demo_Title":true}}];
    var defaultState = { "name": "default", "bitmask": 0, "nodes":{}, "counts":{"accounts":null, "contacts":null} };
    var undefinedState = { "name": "default", "bitmask": -1, "nodes":{}, "counts":{"accounts":0, "contacts":0} };
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

    this.loadData = function() {
        var self = this;
        this.validResourceTypes.forEach(function(resourceType) {
            self.setResourceTypeCount(resourceType, true);
        });

        return $http({
            method: 'GET',
            url: 'assets/resources/stub/records.json',
        }).then(function(result) {
            var data = result.data || {}
            for (var resourceType in data) {
                self.columns[resourceType] = data[resourceType].columns || [];
                self.records[resourceType] = data[resourceType].records || [];

                if (self.uiState === defaultState) {
                    self.uiState.counts[resourceType] = self.records[resourceType].length;
                }
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

        for (var i = 0; i < this.validResourceTypes.length; i++){
            var resourceType = this.validResourceTypes[i];
            this.setResourceTypeCount(resourceType, true);

            if (timeout[resourceType]) {
                $timeout.cancel(timeout[resourceType]);
            }

            timeout[resourceType] = $timeout((function(x) {
                return function() {
                    self.setResourceTypeCount(x, false, self.uiState.counts[x]);
                }
            })(resourceType), delay + delay * Math.random());
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
                var columnName = BucketRestriction.getColumnName(group[i].bucketRestriction);

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

    this.getRecordsForUiState = function(resourceType) {
        if (!this.isValidResourceType(resourceType)) {
            return [];
        }

        var bitmask = this.uiState.bitmask;

        return this.records[resourceType].filter(function(record) {
            return (record.uiState & bitmask) === bitmask;
        });
    };

});
