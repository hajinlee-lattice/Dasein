angular.module('common.datacloud.query.service',[
])
.service('QueryStore', function($filter, $q, $stateParams, $timeout, QueryService, BucketRestriction) {

    angular.extend(this, {});

    this.validResourceTypes = ['accounts', 'contacts'];
    this.segment = null;
    
    this.validContexts = ['accounts', 'contacts'];
    var allRestrictions = [];
    var anyRestrictions = [];
    this.restriction = {
        "restriction": {
            "logicalRestriction": {
                "operator": "AND",
                "restrictions": [
                    {
                        "logicalRestriction": {
                            "operator": "AND",
                            "restrictions": allRestrictions
                        }
                    },
                    {
                        "logicalRestriction": {
                            "operator": "OR",
                            "restrictions": anyRestrictions
                        }
                    }
                ]
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
        allRestrictions = restriction.all;
        anyRestrictions = restriction.any;
    };

    this.setSegment = function(segment) {
        this.segment = segment;
    };

    this.getSegment = function() {
        return this.segment;
    };

    this.setupStore = function(segment) {

        // console.log(segment);

        var self = this;
        var deferred = $q.defer();

        this.setSegment(segment);
        if (segment !== null) {
            this.setRestriction(segment.frontend_restriction);
            deferred.resolve();
        } else {
            this.setRestriction({"restriction": {"logicalRestriction": {"operator": "AND","restrictions": [{"logicalRestriction": {"operator": "AND","restrictions": allRestrictions }},{"logicalRestriction": {"operator": "OR","restrictions": anyRestrictions }}]}}});
            deferred.resolve();
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

        attribute.resourceType = attribute.resourceType || 'LatticeAccount';
        attribute.attr = attribute.resourceType + '.' + attribute.columnName;

        console.log(attribute);

        allRestrictions.push({
            bucketRestriction: new BucketRestriction(attribute.columnName, attribute.resourceType, attribute.bkt.Rng, attribute.attr, attribute.bkt)
        });

        $stateParams.accountCount = this.GetCountByQuery('accounts', '');
        $stateParams.loadingData = true;

        console.log(allRestrictions);


        // var attributesFound = this.findAttributes(attribute.columnName);
        // var attributes = attributesFound.attributes;
        // var groupKey = attributesFound.groupKey;
        // var found = false;
        // for (var i = 0; i < attributes.length; i++) {
        //     var attributeMeta = attributes[i];
        //     if (BucketRestriction.isEqualRange(attribute.bkt.Rng, BucketRestriction.getRange(attributeMeta.bucketRestriction))) {
        //         found = true;
        //         break;
        //     }
        // }
        // if (!found) {
        //     groupKey = groupKey || {};
        //     this.restriction[groupKey].push({
        //         bucketRestriction: new BucketRestriction(attribute.columnName, attribute.resourceType, attribute.attr, attribute.bkt)
        //     });
        //     // this.updateUiState(attribute.columnName, 1, this.restriction.all.length + this.restriction.any.length);
        // }

    };

    this.removeRestriction = function(attribute) {

        var index = allRestrictions.indexOf({ 'bucketRestriction': attribute });

        console.log(index);
        allRestrictions.splice(index, 1);

        console.log(allRestrictions);


        // var attributesFound = this.findAttributes(attribute.attr);
        // var attributes = attributesFound.attributes;
        // var groupKey = attributesFound.groupKey;
        // for (var i = 0; i < attributes.length; i++) {
        //     var attributeMeta = attributes[i];

        //     var columnName = BucketRestriction.getColumnName(attributeMeta.bucketRestriction);
        //     if (attribute.columnName === columnName &&
        //         BucketRestriction.isEqualRange(attribute.range, BucketRestriction.getRange(attributeMeta.bucketRestriction))) {
        //         allRestrictions.splice(attributeMeta.index, 1);
        //         break;
        //     }
        // }

        

    };

    this.findAttributes = function(columnName) {

        var groupKey = null;
        var attributes = [];

        for (var group in this.restriction) {
            var attributes = this.findAttributesInGroup(group, columnName);
            
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
            console.log(group, "fired");
            if (group[i].bucketRestriction.columnName === columnName) {
                results.push({index: i, bucketRestriction: group[i].bucketRestriction });
            }
        }
        return results;
    };


    this.GetCountByQuery = function(resourceType, query) {
        
        if (!this.isValidResourceType(resourceType)) {
            var deferred = $q.defer();
            deferred.resolve({error: {errMsg:'Invalid resourceType: ' + resourceType} });
            return deferred.promise;
        } else {
            var queryWithRestriction = { 
                'free_form_text_search': query,
                'frontend_restriction': this.restriction,
                'page_filter': {
                    'num_rows': 10,
                    'row_offset': 0
                }
            };
            return QueryService.GetCountByQuery(resourceType, queryWithRestriction);
        }

    };

    this.GetDataByQuery = function(resourceType, query, segment) {
        
        if (!this.isValidResourceType(resourceType)) {
            var deferred = $q.defer();
            deferred.resolve({error: {errMsg:'Invalid resourceType: ' + resourceType} });
            return deferred.promise;
        } else {

            console.log(query);

            var queryWithRestriction = { 
                'free_form_text_search': query.free_form_text_search,
                'frontend_restriction': this.restriction,
                'page_filter': {
                    'num_rows': query.page_filter.num_rows,
                    'row_offset': query.page_filter.row_offset
                }
            };
            return QueryService.GetDataByQuery(resourceType, queryWithRestriction);
        }

        return QueryService.GetDataByQuery(resourceType, query, segment);
    };

    this.isValidResourceType = function(resourceType) {
        return this.validResourceTypes.indexOf(resourceType) > -1;
    };


})
.service('QueryService', function($http, $q) {

    this.GetCountByRestriction = function(resourceType, restriction) {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/' + resourceType + '/count/restriction',
            data: restriction
        }).success(function(result) {
            deferred.resolve(result);
        }).error(function(result) {
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.GetCountByQuery = function(resourceType, query) {
        var deferred = $q.defer();
        $http({
            method: 'POST',
            url: '/pls/' + resourceType + '/count',
            data: query
        }).success(function(result) {
            deferred.resolve(result);
        }).error(function(result) {
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.GetDataByQuery = function(resourceType, query, segment) {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/' + resourceType + '/data',
            data: query,
            params: {
                segment: segment
            }
        }).success(function(result) {
            deferred.resolve(result);
        }).error(function(result) {
            deferred.resolve(result);
        });

        return deferred.promise;
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
