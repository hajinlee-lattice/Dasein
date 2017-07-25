angular.module('common.datacloud.query.service',[
])
.service('QueryStore', function($filter, $q, $timeout, QueryService, BucketRestriction) {

    angular.extend(this, {});

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

    this.adjustCounts = function() {

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
        attribute.resourceType = attribute.resourceType || 'Account';

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

        console.log(attribute);

        if (!found) {
            groupKey = groupKey || 'all';
            this.restriction[groupKey].push({
                bucketRestriction: new BucketRestriction(attribute.columnName, attribute.resourceType, attribute.range, attribute.attr, attribute.bkt)
            });
        }


    };

    this.removeRestriction = function(attribute) {
        var attributesFound = this.findAttributes(attribute.columnName);
        var attributes = attributesFound.attributes;
        var groupKey = attributesFound.groupKey;

        for (var i = 0; i < attributes.length; i++) {
            var attributeMeta = attributes[i];
            var columnName = BucketRestriction.getColumnName(attributeMeta.bucketRestriction);
            if (attribute.columnName === columnName &&
                BucketRestriction.isEqualRange(attribute.range, BucketRestriction.getRange(attributeMeta.bucketRestriction))) {
                this.restriction[groupKey].splice(attributeMeta.index, 1);
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
            console.log(response);
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
});
// .service('QueryServiceStub', function($http, $timeout, $q, BucketRestriction) {

//     // var uiStates = [{"name":"1","bitmask":2048,"counts":{"accounts":38727,"contacts":202572},"nodes":{"Demo - Other - Title":true}},{"name":"2","bitmask":1024,"counts":{"accounts":32769,"contacts":61566},"nodes":{"Demo - Other - Department":true,"Demo - Other - Title":true}},{"name":"3","bitmask":512,"counts":{"accounts":32769,"contacts":60573},"nodes":{"Demo - Other - Is With Company":true,"Demo - Other - Department":true,"Demo - Other - Title":true}},{"name":"4","bitmask":256,"counts":{"accounts":8937,"contacts":15888},"nodes":{"Demo - Abrasives - Abrasives: Has Purchased":true,"Demo - Other - Is With Company":true,"Demo - Other - Department":true,"Demo - Other - Title":true}},{"name":"5","bitmask":128,"counts":{"accounts":1986,"contacts":3972},"nodes":{"Lattice_Ratings":true,"Demo - Abrasives - Abrasives: Has Purchased":true,"Demo - Other - Is With Company":true,"Demo - Other - Department":true,"Demo - Other - Title":true}},{"name":"6","bitmask":64,"counts":{"accounts":3972,"contacts":8937},"nodes":{"Demo - Abrasives - Abrasives: Last Qtr Trend (%)":true,"Demo - Other - Is With Company":true,"Demo - Other - Department":true,"Demo - Other - Title":true}},{"name":"7","bitmask":32,"counts":{"accounts":22839,"contacts":251229},"nodes":{"TechIndicator_AmazonEC2":true}},{"name":"8","bitmask":16,"counts":{"accounts":5958,"contacts":65538},"nodes":{"Demo - Other - Cloud Security Intent":true,"TechIndicator_AmazonEC2":true}},{"name":"9","bitmask":8,"counts":{"accounts":2979,"contacts":32769},"nodes":{"Demo - Other - Anonymous Engagement Rating":true,"Demo - Other - Cloud Security Intent":true,"TechIndicator_AmazonEC2":true}},{"name":"10","bitmask":4,"counts":{"accounts":27804,"contacts":216474},"nodes":{"Demo - Other - Has Opportunity":true,"Demo - Other - Is With Company":true,"Demo - Other - Department":true,"Demo - Other - Title":true}},{"name":"11","bitmask":2,"counts":{"accounts":20853,"contacts":140013},"nodes":{"Demo - Other - Known Contact Engagement":true,"Demo - Other - Has Opportunity":true,"Demo - Other - Is With Company":true,"Demo - Other - Department":true,"Demo - Other - Title":true}},{"name":"12","bitmask":1,"counts":{"accounts":2979,"contacts":16881},"nodes":{"TechIndicator_AmazonEC2":true,"Demo - Other - Known Contact Engagement":true,"Demo - Other - Has Opportunity":true,"Demo - Other - Is With Company":true,"Demo - Other - Department":true,"Demo - Other - Title":true}}];
//     // var defaultState = { "name": "0", "bitmask": 0, "nodes":{}, "counts":{"accounts":null, "contacts":null} };
//     // var undefinedState = { "name": "-1", "bitmask": -1, "nodes":{}, "counts":{"accounts":0, "contacts":0} };
//     var self = this;

//     this.columns = {
//         accounts: [],
//         contacts: []
//     };

//     this.records = {
//         accounts: [],
//         contacts: []
//     };

//     this.uiState = defaultState;
//     this.uiStateMap = {};

//     this.setUiState = function(uiState) {
//         this.uiState = uiState;
//         this.updateCount();
//     };

//     this.getUiState = function() {
//         return this.uiState;
//     };

//     var timeout = {
//         accounts: null,
//         contacts: null
//     };

//     this.updateCount = function() {
//         var self = this;
//         var delay = 500;

//         for (var i = 0; i < this.validResourceTypes.length; i++){
//             var resourceType = this.validResourceTypes[i];
//             this.setResourceTypeCount(resourceType, true);

//             if (timeout[resourceType]) {
//                 $timeout.cancel(timeout[resourceType]);
//             }

//             timeout[resourceType] = $timeout((function(x) {
//                 return function() {
//                     self.setResourceTypeCount(x, false, self.uiState.counts[x]);
//                 }
//             })(resourceType), delay + delay * Math.random());
//         }
//     };

//     this.updateUiStateMapAndGetState = function(columnName, step, totalLen) {
//         var uiStateFound = null;
//         for (var j = 0; j < uiStates.length; j++) {
//             var uiState = uiStates[j];
//             if (typeof uiState.nodes[columnName] !== 'undefined') {
//                 this.uiStateMap[uiState.name] += step;
//             }
//             if (this.uiStateMap[uiState.name] === Object.keys(uiState.nodes).length &&
//                 this.uiStateMap[uiState.name] === totalLen) {
//                 uiStateFound = uiState;
//             }
//         }
//         return uiStateFound;
//     };

//     this.initUiStates = function(restriction) {
//         for (var i = 0; i < uiStates.length; i++) {
//             this.uiStateMap[uiStates[i].name] = 0;
//         }

//         var totalLen = restriction.all.length + restriction.any.length;
//         if (totalLen === 0) {
//             this.setUiState(defaultState);
//             return;
//         }

//         var uiStateFound = null;
//         for (var groupKey in restriction) {
//             var group = restriction[groupKey];
//             for (var i = 0; i < group.length; i++) {
//                 var columnName = BucketRestriction.getColumnName(group[i].bucketRestriction);

//                 uiStateFound = this.updateUiStateMapAndGetState(columnName, 1, totalLen);
//             }
//         }

//         this.setUiState(uiStateFound || undefinedState);
//     };

//     this.updateUiState = function(columnName, step, totalLen) { // step +1 | -1
//         var uiStateFound = this.updateUiStateMapAndGetState(columnName, step, totalLen);

//         if (uiStateFound === null && totalLen === 0) {
//             uiStateFound = defaultState;
//         }
//         this.setUiState(uiStateFound || undefinedState);
//     };

//     this.getRecordsForUiState = function(resourceType) {
//         if (!this.isValidResourceType(resourceType)) {
//             return [];
//         }

//         var bitmask = this.uiState.bitmask;

//         return this.records[resourceType].filter(function(record) {
//             return (record.uiState & bitmask) === bitmask;
//         });
//     };

// });
