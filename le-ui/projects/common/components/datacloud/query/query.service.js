angular.module('common.datacloud.query.service',[
])
.service('QueryStore', function($filter, $q, $stateParams, $timeout, QueryService, BucketRestriction, SegmentStore) {
    var QueryStore = this;

    angular.extend(this, {});
    this.validResourceTypes = ['accounts', 'contacts', 'products'];
    this.segment = null;

    // for Adanced Query Builder
    this.history = [];

    this.validContexts = ['accounts', 'contacts', 'products'];

    this.counts = {
        accounts: {
            value: 0,
            loading: false
        },
        contacts: {
            value: 0,
            loading: false
        },
        product: {
            value: 0,
            loading: false
        }
    };
    
    this.accounts = [];
    this.contacts = [];

    this.public = {
        enableSaveSegmentButton: false,
        resetLabelIncrementor: false
    };

    this.bucketsToLaunch = null;
    this.ratedTargetsLimit = null;

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

    this.setEntitiesProperty = function(property, value) {
        for (var key in this.counts) {
            this.counts[key][property] = value;
        }
    }

    this.setBucketsToLaunch = function(buckets) {
        this.bucketsToLaunch = buckets;
    }

    this.getBucketsToLaunch = function() {
        return this.bucketsToLaunch;
    }

    this.setRatedTargetsLimit = function(limit) {
        this.ratedTargetsLimit = limit;
    }

    this.getRatedTargetsLimit = function() {
        return this.ratedTargetsLimit;
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

    // this.setPurchaseHistoryRestriction = function(purchaseRestriction){
    //     if (purchaseRestriction) {
    //         this.purchaseRestriction = purchaseRestriction;
    //     }
    // }
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

            // this.setPurchaseHistoryRestriction({
            //     "restriction": {
            //         "logicalRestriction": {
            //             "operator": "AND",
            //             "restrictions": purchaseRestriction 
            //         }
            //     }
            // });

            accountRestrictionsString = JSON.stringify(this.getAccountRestriction());
            contactRestrictionsString = JSON.stringify(this.getContactRestriction());
            defaultRestrictions = accountRestrictionsString + contactRestrictionsString;
            this.setDefaultRestrictions(defaultRestrictions);

            deferred.resolve();

        }

        return deferred.promise;
    }

    this.getSegmentProperty = function(properties, propertyName) {
        for (var i = 0; i < properties.length; i++) {
            var property = properties[i].metadataSegmentProperty;
            if (property.option === propertyName) {
                return property.value;
            }
        }

        return null;
    }

    this.setAddBucketTreeRoot = function(tree) {
        if (tree === null) {
            delete this.addBucketTreeRoot;
        } else {
            this.addBucketTreeRoot = tree;
        }
    }

    this.getAddBucketTreeRoot = function(tree) {
        return this.addBucketTreeRoot;
    }

    this.addAccountRestriction = function(attribute) {
        this.addRestriction('account', attribute);
    }

    this.addContactRestriction = function(attribute) {
        this.addRestriction('contact', attribute);
    }

    this.addPurchaseHistoryRestriction = function(attribute) {
        this.addRestriction('account', attribute);
    }

    this.removeAccountRestriction = function(attribute) {
        this.removeRestriction('account', attribute);
    }

    this.removeContactRestriction = function(attribute) {
        this.removeRestriction('contact', attribute);
    }

    this.addRestriction = function(type, attribute) {
        attribute = this.setAttributeAttr(type, attribute);

        var treeRoot = this.getAddBucketTreeRoot(),
            restrictions = treeRoot 
                ? treeRoot.logicalRestriction.restrictions 
                : this[type + 'Restriction'].restriction.logicalRestriction.restrictions;
                
        restrictions.push({
            bucketRestriction: new BucketRestriction(attribute.columnName, attribute.resourceType, attribute.bkt.Rng, attribute.attr, attribute.bkt)
        });

        this.setRestrictions(type);
    }

    this.removeRestriction = function(type, attribute) {
        attribute = this.setAttributeAttr(type, attribute);

        var searchTerm = attribute.attr,
            index = -1,
            retrictions = this[type + 'Restriction'].restriction.logicalRestriction.restrictions;
            
        for (var i = 0, len = retrictions.length; i < len; i++) {
            if (retrictions[i].bucketRestriction && retrictions[i].bucketRestriction.attr === searchTerm) {
                var index = i;
                break;
            }
        }

        if (index >= 0) {
            retrictions.splice(index, 1);
        }

        this.setRestrictions(type);
    }

    this.setAttributeAttr = function(type, attribute) {
        var resourceType = type == 'contact' ? 'Contact' : 'LatticeAccount';
        attribute.resourceType = attribute.resourceType || resourceType;
        attribute.attr = attribute.resourceType + '.' + attribute.columnName;
        return attribute;
    }

    this.setRestrictions = function(type) {
        type == 'account'
            ? this.setAccountRestriction(this.accountRestriction)
            : this.setContactRestriction(this.contactRestriction);

        this.getEntitiesCounts();
    }

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

    this.getEntitiesCounts = function() {
        var deferred = $q.defer();

        this.GetEntitiesCountsByQuery().then(function(data){
            QueryStore.setResourceTypeCount('accounts', false, data['Account']);
            QueryStore.setResourceTypeCount('contacts', false, data['Contact']);
            QueryStore.setEntitiesProperty('loading', false);

            deferred.resolve(data);
        });

        return deferred.promise;
    };

    this.GetEntitiesCountsByQuery = function(query) {  
        var deferred = $q.defer(),
            accountRestriction = this.getAccountRestriction(),
            contactRestriction = this.getContactRestriction();

        if (query === undefined || query === ''){
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

        QueryService.GetEntitiesCounts(queryWithRestriction).then(function(data) {
            deferred.resolve(data);
        });

        return deferred.promise;
    };

    this.GetCountByQuery = function(resourceType, query) {  
        if (!this.isValidResourceType(resourceType)) {
            var deferred = $q.defer();
            
            deferred.resolve({
                error: {
                   errMsg:'Invalid resourceType: ' + resourceType
                }
            });

            return deferred.promise;
        } else {
            var deferred = $q.defer(),
                accountRestriction = this.getAccountRestriction(),
                contactRestriction = this.getContactRestriction();

            if (query === undefined || query === ''){
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

            QueryService.GetCountByQuery(resourceType, queryWithRestriction).then(function(data) {
                deferred.resolve(data[resourceType == 'account' ? 'Account' : 'Contact']);
            });

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

    this.getAllBuckets = function(tree, restrictions, getEmptyBuckets) {
        restrictions = restrictions || [];

        tree.forEach(function(branch) {
            if (branch && branch.bucketRestriction && branch.bucketRestriction) {
                if (getEmptyBuckets || typeof branch.bucketRestriction.bkt.Id == 'number') {
                    restrictions.push(branch);
                }
            }

            if (branch && branch.logicalRestriction) {
                QueryStore.getAllBuckets(branch.logicalRestriction.restrictions, restrictions);
            }
        });

        return restrictions;
    };

    this.getDataCloudAttributes = function(ignoreCache) {
        var treeRoot = QueryStore.getAddBucketTreeRoot();
        var restrictions = [];

        if (treeRoot) {
            this.getAllBuckets(treeRoot.logicalRestriction.restrictions, restrictions);
        } else {
            var ar = QueryStore.getAccountRestriction();
            var cr = QueryStore.getContactRestriction();

            this.getAllBuckets(ar.restriction.logicalRestriction.restrictions, restrictions);
            this.getAllBuckets(cr.restriction.logicalRestriction.restrictions, restrictions);
        }

        return restrictions;
    }

    this.generateBucketLabel = function(bkt) {
        switch (bkt.Cmp) {
            case 'Yes': bkt.Lbl = 'Yes'; break;
            case 'empty': bkt.Lbl = ''; break;
            case 'between': bkt.Lbl = bkt.Vals[0] + ' - ' + bkt.Vals[1]; break;
            case 'IS_NULL': bkt.Lbl = ''; break;
            case 'IS_NOT_NULL': bkt.Lbl = ''; break;
            case 'GREATER_THAN': bkt.Lbl = '> ' + bkt.Vals[0]; break;
            case 'LESS_THAN': bkt.Lbl = '< ' + bkt.Vals[0]; break;
            case 'GREATER_OR_EQUAL': bkt.Lbl = '>= ' + bkt.Vals[0]; break;
            case 'LESS_OR_EQUAL': bkt.Lbl = '<= ' + bkt.Vals[0]; break;
            case 'GTE_AND_LTE': bkt.Lbl = '>= ' + bkt.Vals[0] + ' and <= ' + bkt.Vals[1]; break;
            case 'GTE_AND_LT': bkt.Lbl = '>= ' + bkt.Vals[0] + ' and < ' + bkt.Vals[1]; break;
            case 'GT_AND_LTE': bkt.Lbl = '> ' + bkt.Vals[0] + ' and <= ' + bkt.Vals[1]; break;
            case 'GT_AND_LT': bkt.Lbl = '> ' + bkt.Vals[0] + ' and < ' + bkt.Vals[1]; break;
            default:
                bkt.Lbl = (bkt.Vals && bkt.Vals.length>0) ? bkt.Vals[0] : 'empty';
        }

        return bkt;
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

    this.GetEntitiesCounts = function(query, cancelPrevious) {
        var deferred = $q.defer();

        if (this.canceler && cancelPrevious) {
            this.canceler.resolve("cancelled");
        }
        
        this.canceler = $q.defer(); 

        SegmentStore.sanitizeSegment(query);

        $http({
            method: 'POST',
            url: '/pls/entities/counts',
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