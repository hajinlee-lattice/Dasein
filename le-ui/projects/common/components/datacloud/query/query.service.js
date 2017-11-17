angular.module('common.datacloud.query.service',[
])
.service('QueryStore', function($filter, $q, $stateParams, $timeout, QueryService, BucketRestriction, SegmentStore) {
    var QueryStore = this;

    angular.extend(this, {});
    this.validResourceTypes = ['accounts', 'contacts'];
    this.segment = null;

    // for Adanced Query Builder
    this.addBucketTreeRoot = null;
    this.history = [];

    this.validContexts = ['accounts', 'contacts'];

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
    this.contacts = [];

    this.public = {
        enableSaveSegmentButton: false
    };

    this.init = function() {
        this.resetRestrictions();
    }

    this.resetRestrictions = function() {
        this.accountRestriction = {
            "restriction": {
                "logicalRestriction": {
                    "operator": "AND",
                    "restrictions": [] 
                }
            }
        };
        this.contactRestriction = {
            "restriction": {
                "logicalRestriction": {
                    "operator": "AND",
                    "restrictions": []
                }
            }
        };
    }

    this.setPublicProperty = function(property, value) {
        this.public[property] = value;
    }

    this.getPublic = function() {
        return this.public;
    }

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
    
    this.setAccounts = function(query) {
        var deferred = $q.defer();

        this.GetDataByQuery('accounts', query).then(function(response) {
            this.accounts = response;
            deferred.resolve(response);
        });
        return deferred.promise;
    };

    this.getAccounts = function(){ 
        return this.accounts;
    };


    this.setContacts = function(query) {
        var deferred = $q.defer();

        this.GetDataByQuery('contacts', query).then(function(response) {
            this.contacts = response;
            deferred.resolve(response);
        });
        return deferred.promise;
    };

    this.getContacts = function(){ 
        return this.contacts;
    };


    var self = this;
    this.validResourceTypes.forEach(function(resourceType) {
        self.setResourceTypeCount(resourceType, true);
    });

    this.setDefaultRestrictions = function(defaultRestrictions) {
        this.defaultRestrictions = defaultRestrictions;
    };
    this.getDefaultRestrictions = function() {
        return this.defaultRestrictions;
    };

    this.setAccountRestriction = function(accountRestriction) {
        if (accountRestriction) {
            this.accountRestriction = accountRestriction;
        }
    };
    this.getAccountRestriction = function() {
        return this.accountRestriction;
    };
    this.updateAccountRestriction = function(accountRestriction) {
        //accountRestriction = accountRestriction.all;
    };

    this.setContactRestriction = function(contactRestriction) {
        if (contactRestriction) {
            this.contactRestriction = contactRestriction;
        }
    };
    this.getContactRestriction = function() {
        return this.contactRestriction;
    };
    this.updateContactRestriction = function(contactRestriction) {
        //contactRestriction = contactRestriction.all;
    };

    this.setSegment = function(segment) {
        this.segment = segment;
    };

    this.getSegment = function() {
        return this.segment;
    };

    this.setupStore = function(segment) {
        var self = this,
            deferred = $q.defer();

        this.setSegment(segment)

        if (segment != null) {
            accountRestriction = segment.account_restriction ? segment.account_restriction : [];
            contactRestriction = segment.contact_restriction ? segment.contact_restriction : [];

            this.setAccountRestriction(segment.account_restriction);
            this.setContactRestriction(segment.contact_restriction);

            accountRestrictionsString = JSON.stringify(accountRestriction);
            contactRestrictionsString = JSON.stringify(contactRestriction);
            defaultRestrictions = accountRestrictionsString + contactRestrictionsString;

            this.setDefaultRestrictions(defaultRestrictions);

            deferred.resolve();

        } else {
            accountRestriction = [];
            contactRestriction = [];

            this.setAccountRestriction({
                "restriction": {
                    "logicalRestriction": {
                        "operator": "AND",
                        "restrictions": accountRestriction 
                    }
                }
            });
            this.setContactRestriction({
                "restriction": {
                    "logicalRestriction": {
                        "operator": "AND",
                        "restrictions": contactRestriction 
                    }
                }
            });

            accountRestrictionsString = JSON.stringify(this.getAccountRestriction());
            contactRestrictionsString = JSON.stringify(this.getContactRestriction());
            defaultRestrictions = accountRestrictionsString + contactRestrictionsString;
            this.setDefaultRestrictions(defaultRestrictions);

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

    this.setAddBucketTreeRoot = function(tree) {
        this.addBucketTreeRoot = tree;
    }

    this.getAddBucketTreeRoot = function(tree) {
        return this.addBucketTreeRoot;
    }

    this.addAccountRestriction = function(attribute) {
        attribute.resourceType = attribute.resourceType || 'LatticeAccount';
        attribute.attr = attribute.resourceType + '.' + attribute.columnName;

        var treeRoot = this.getAddBucketTreeRoot(),
            restrictions = treeRoot 
                ? treeRoot.logicalRestriction.restrictions 
                : this.accountRestriction.restriction.logicalRestriction.restrictions;
        
        restrictions.push({
            bucketRestriction: new BucketRestriction(attribute.columnName, attribute.resourceType, attribute.bkt.Rng, attribute.attr, attribute.bkt)
        });

        this.setAccountRestriction(this.accountRestriction);

        var self = this;
        this.GetCountByQuery('accounts').then(function(data){
            self.setResourceTypeCount('accounts', false, data);
            self.counts.accounts.loading = false;
        });
        this.GetCountByQuery('contacts').then(function(data){
            self.setResourceTypeCount('contacts', false, data);
            self.counts.contacts.loading = false;
        });

    };

    this.removeAccountRestriction = function(attribute) {
        attribute.resourceType = attribute.resourceType || 'LatticeAccount';
        attribute.attr = attribute.resourceType + '.' + attribute.columnName;

        var searchTerm = attribute.attr,
            index = -1,
            retrictions = this.accountRestriction.restriction.logicalRestriction.restrictions;

        for (var i = 0, len = retrictions.length; i < len; i++) {
            if (retrictions[i].bucketRestriction && retrictions[i].bucketRestriction.attr === searchTerm) {
                var index = i;
                break;
            }
        }

        if (index >= 0) {
            retrictions.splice(index, 1);
        }

        this.setAccountRestriction(this.accountRestriction);

        var self = this;
        this.GetCountByQuery('accounts').then(function(data){
            self.setResourceTypeCount('accounts', false, data);
            self.counts.accounts.loading = false;
        });
        this.GetCountByQuery('contacts').then(function(data){
            self.setResourceTypeCount('contacts', false, data);
            self.counts.contacts.loading = false;
        });
    };

    this.addContactRestriction = function(attribute) {
        attribute.resourceType = attribute.resourceType || 'LatticeAccount';
        attribute.attr = attribute.resourceType + '.' + attribute.columnName;

        var treeRoot = this.getAddBucketTreeRoot(),
            restrictions = treeRoot 
                ? treeRoot.logicalRestriction.restrictions 
                : this.contactRestriction.restriction.logicalRestriction.restrictions;
        
        restrictions.push({
            bucketRestriction: new BucketRestriction(attribute.columnName, attribute.resourceType, attribute.bkt.Rng, attribute.attr, attribute.bkt)
        });

        this.setContactRestriction(this.contactRestriction);

        var self = this;
        this.GetCountByQuery('accounts').then(function(data){
            self.setResourceTypeCount('accounts', false, data);
            self.counts.accounts.loading = false;
        });
        this.GetCountByQuery('contacts').then(function(data){
            self.setResourceTypeCount('contacts', false, data);
            self.counts.contacts.loading = false;
        });
    };

    this.removeContactRestriction = function(attribute) {
        attribute.resourceType = attribute.resourceType || 'LatticeAccount';
        attribute.attr = attribute.resourceType + '.' + attribute.columnName;

        var searchTerm = attribute.attr,
            index = -1,
            retrictions = this.contactRestriction.restriction.logicalRestriction.restrictions;

        for (var i = 0, len = retrictions.length; i < len; i++) {
            if (retrictions[i].bucketRestriction && retrictions[i].bucketRestriction.attr === searchTerm) {
                var index = i;
                break;
            }
        }

        if (index >= 0) {
            retrictions.splice(index, 1);
        }

        this.setContactRestriction(this.contactRestriction);

        var self = this;
        this.GetCountByQuery('accounts').then(function(data){
            self.setResourceTypeCount('accounts', false, data);
            self.counts.accounts.loading = false;
        });
        this.GetCountByQuery('contacts').then(function(data){
            self.setResourceTypeCount('contacts', false, data);
            self.counts.contacts.loading = false;
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

            var deferred = $q.defer(),
                accountRestriction = this.getAccountRestriction(),
                contactRestriction = this.getContactRestriction();

            if(query === undefined || query === ''){
                var queryWithRestriction = { 
                    'free_form_text_search': '',
                    'account_restriction': accountRestriction,
                    'contact_restriction': contactRestriction,
                    'restrict_without_sfdcid': false,
                    'page_filter': {
                        'num_rows': 10,
                        'row_offset': 0
                    }
                };

            } else {
                var queryWithRestriction = { 
                    'free_form_text_search': query.free_form_text_search || '',
                    'account_restriction': query.account_restriction || {},
                    'contact_restriction': query.contact_restriction || {},
                    'preexisting_segment_name': query.preexisting_segment_name,
                    'page_filter': {
                        'num_rows': query.page_filter.num_rows,
                        'row_offset': query.page_filter.row_offset
                    },
                    'restrict_without_sfdcid': query.restrict_without_sfdcid
                };
            };

            queryWithRestriction = SegmentStore.sanitizeSegment(queryWithRestriction);

            deferred.resolve(QueryService.GetCountByQuery(resourceType, queryWithRestriction));
            return deferred.promise;
        }
    };

    this.GetDataByQuery = function(resourceType, query) {  
        if (!this.isValidResourceType(resourceType)) {
            var deferred = $q.defer();
            deferred.resolve({error: {errMsg:'Invalid resourceType: ' + resourceType} });
            return deferred.promise;
        } else {

            var deferred = $q.defer(),
                queryWithRestriction = { 
                    'free_form_text_search': query.free_form_text_search,
                    'account_restriction': query.account_restriction,
                    'contact_restriction': query.contact_restriction,
                    'preexisting_segment_name': query.preexisting_segment_name,
                    'page_filter': {
                        'num_rows': query.page_filter.num_rows,
                        'row_offset': query.page_filter.row_offset
                    },
                    'restrict_with_sfdcid': query.restrict_with_sfdcid
                };

            deferred.resolve(QueryService.GetDataByQuery(resourceType, queryWithRestriction));
            return deferred.promise;
        }
    };

    this.isValidResourceType = function(resourceType) {
        return this.validResourceTypes.indexOf(resourceType) > -1;
    };

    this.getAllBuckets = function(tree, restrictions) {
        restrictions = restrictions || [];

        tree.forEach(function(branch) {
            if (branch && branch.bucketRestriction && branch.bucketRestriction && typeof branch.bucketRestriction.bkt.Id == 'number') {
                restrictions.push(branch);
            }

            if (branch && branch.logicalRestriction) {
                QueryStore.getAllBuckets(branch.logicalRestriction.restrictions, restrictions);
            }
        });

        return restrictions;
    };

    this.getDataCloudAttributes = function(ignoreCache) {
        //if (ignoreCache) {
            var restrictions = [];
            var ar = QueryStore.getAccountRestriction();
            var cr = QueryStore.getContactRestriction();

            this.getAllBuckets(ar.restriction.logicalRestriction.restrictions, restrictions);
            this.getAllBuckets(cr.restriction.logicalRestriction.restrictions, restrictions);

        //}

        return restrictions;
    }

    this.init();
})
.service('QueryService', function($http, $q, SegmentStore) {
    this.canceler = null;

    this.GetCountByQuery = function(resourceType, query, cancelPrevious) {
        if (this.canceler && cancelPrevious) {
            this.canceler.resolve("cancelled");
        }
        
        this.canceler = $q.defer(); 
        var deferred = $q.defer(); 

        SegmentStore.sanitizeSegment(query);

        $http({
            method: 'POST',
            url: '/pls/' + resourceType + '/count',
            data: query,
            timeout: this.canceler.promise,
            headers: {
                'ErrorDisplayMethod': 'none'
            }
        }).success(function(result) {
            deferred.resolve(result);
        }).error(function(result) {
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.GetDataByQuery = function(resourceType, query) {
        var deferred = $q.defer();

        SegmentStore.sanitizeSegment(query);

        $http({
            method: 'POST',
            url: '/pls/' + resourceType + '/data',
            data: query
        }).success(function(result) {
            deferred.resolve(result);
        }).error(function(result) {
            deferred.resolve(result);
        });

        return deferred.promise;
    };
});