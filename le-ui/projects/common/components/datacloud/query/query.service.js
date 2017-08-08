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
    this.accounts = [];

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

    this.getCounts = function() {
        return this.counts;
    };
    
    this.setAccounts = function(query, segment) {
        var deferred = $q.defer();
        this.GetDataByQuery('accounts', query, segment).then(function(response) {
            this.accounts = response;
            deferred.resolve(response);
        });
        return deferred.promise;
    };

    this.getAccounts = function(){ 
        return this.accounts;
    };    

    var self = this;
    this.validResourceTypes.forEach(function(resourceType) {
        self.setResourceTypeCount(resourceType, true);
    });



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

        var self = this;
        var deferred = $q.defer();
        
        if (segment != null) {

            // set segment if clicking on a tile.
            this.setSegment(segment);

            // Set variables so I can manipulate later when unchecking box.
            allRestrictions = segment.frontend_restriction.restriction.logicalRestriction.restrictions[0].logicalRestriction.restrictions;
            anyRestrictions = segment.frontend_restriction.restriction.logicalRestriction.restrictions[1].logicalRestriction.restrictions;

            // Set restriction to get counts and data as part of the query.
            deferred.resolve( this.setRestriction(segment.frontend_restriction) );

        } else {

            // default state. restriction is empty.
            deferred.resolve( this.setRestriction({"restriction": {"logicalRestriction": {"operator": "AND","restrictions": [{"logicalRestriction": {"operator": "AND","restrictions": allRestrictions }},{"logicalRestriction": {"operator": "OR","restrictions": anyRestrictions }}]}}})   );

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

        allRestrictions.push({
            bucketRestriction: new BucketRestriction(attribute.columnName, attribute.resourceType, attribute.bkt.Rng, attribute.attr, attribute.bkt)
        });

        var self = this;
        this.GetCountByQuery('accounts').then(function(data){
            console.log();
            self.setResourceTypeCount('accounts', false, data);
        });            
        
    };

    this.removeRestriction = function(attribute) {

        attribute.resourceType = attribute.resourceType || 'LatticeAccount';
        attribute.attr = attribute.resourceType + '.' + attribute.columnName;

        var searchTerm = attribute.attr,
            index = -1;

        console.log(searchTerm, allRestrictions);

        for(var i = 0, len = allRestrictions.length; i < len; i++) {
            if (allRestrictions[i].bucketRestriction.attr === searchTerm) {
                var index = i;
                break;
            }
        }

        allRestrictions.splice(index, 1);
        

        console.log(index, allRestrictions);


        var self = this;
        this.GetCountByQuery('accounts').then(function(data){
            self.setResourceTypeCount('accounts', false, data);
        });     

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

            var deferred = $q.defer();

            if(query === undefined || query === ''){
                
                var queryWithRestriction = { 
                    'free_form_text_search': '',
                    'frontend_restriction': this.restriction,
                    'page_filter': {
                        'num_rows': 20,
                        'row_offset': 0
                    }
                };

            } else {
                var queryWithRestriction = { 
                    'free_form_text_search': query.free_form_text_search,
                    'frontend_restriction': this.restriction,
                    'page_filter': {
                        'num_rows': query.page_filter.num_rows,
                        'row_offset': query.page_filter.row_offset
                    }
                };
            };

            deferred.resolve(QueryService.GetCountByQuery(resourceType, queryWithRestriction));
            return deferred.promise;
        }
    };

    this.GetDataByQuery = function(resourceType, query, segment) {  
        if (!this.isValidResourceType(resourceType)) {
            var deferred = $q.defer();
            deferred.resolve({error: {errMsg:'Invalid resourceType: ' + resourceType} });
            return deferred.promise;
        } else {

            if(query === undefined || query === ''){
                var queryWithRestriction = { 
                    'free_form_text_search': '',
                    'frontend_restriction': this.restriction,
                    'page_filter': {
                        'num_rows': 20,
                        'row_offset': 0
                    }
                };
            } else {
                var queryWithRestriction = { 
                    'free_form_text_search': query.free_form_text_search,
                    'frontend_restriction': this.restriction,
                    'page_filter': {
                        'num_rows': query.page_filter.num_rows,
                        'row_offset': query.page_filter.row_offset
                    }
                };
            };

            // console.log(queryWithRestriction, segment);

            return QueryService.GetDataByQuery(resourceType, queryWithRestriction, segment);
        }
    };

    this.isValidResourceType = function(resourceType) {
        return this.validResourceTypes.indexOf(resourceType) > -1;
    };


})
.service('QueryService', function($http, $q) {


    var canceler = $q.defer();

    this.GetCountByQuery = function(resourceType, query) {

        canceler.resolve("cancelled");
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