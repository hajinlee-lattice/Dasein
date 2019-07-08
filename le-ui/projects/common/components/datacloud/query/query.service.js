angular.module('common.datacloud.query')
    .service('QueryStore', function(
        $filter, $q, $stateParams, $timeout, QueryService, BucketRestriction,
        SegmentStore, NumberUtility
    ) {
        var QueryStore = this;

        this.entities = ['account', 'contact'];
        this.validResourceTypes = ['accounts', 'contacts', 'products'];
        this.validContexts = ['accounts', 'contacts', 'products'];
        this.segment = null;

        // for Adanced Query Builder
        this.history = [];

        this.mode = '';

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

        this.addBucketTreeType = '';

        this.public = {
            enableSaveSegmentButton: false,
            resetLabelIncrementor: false,
            disableAllTreeRestrictions: false
        };

        this.isDataAvailable = null;
        this.collectionStatus = null;

        this.cancelUpdateBucketCalls = false;

        this.init = function() {
            this.initRestrictions();
        }

        function getEntity(entity) {
            switch (entity) {
                case 'account':
                case 'purchasehistory':
                    {
                        return 'account';
                    }
                case 'contact':
                    {
                        return 'contact';
                    }
            }
        }

        this.clear = function() {
            delete this.accountBucketTreeRoot;
            delete this.contactBucketTreeRoot;
            delete this.addBucketTreeRoot;
            this.addBucketTreeType = '';
            this.mode = '';
            this.segment = null;
            this.accounts = [];
            this.contacts = [];
            this.initRestrictions();
            this.selectedBucket = 'A';
        };

        this.initRestrictions = function() {
            var template = {
                restriction: {
                    logicalRestriction: {
                        operator: "AND",
                        restrictions: []
                    }
                }
            };

            this.entities.forEach(function(entity) {
                QueryStore[getEntity(entity) + 'Restriction'] = angular.copy(template);
            });
        }

        this.resetRestrictions = function(segment) {
            this.entities.forEach(function(entity) {
                var restriction = QueryStore[getEntity(entity) + 'Restriction'].restriction.logicalRestriction;

                restriction.operator = "AND";

                if (segment && segment[getEntity(entity) + '_restriction']) {
                    restriction.restrictions = segment[getEntity(entity) + '_restriction'].restriction.logicalRestriction.restrictions;
                    restriction.operator = segment[getEntity(entity) + '_restriction'].restriction.logicalRestriction.operator;
                } else {
                    restriction.restrictions.length = 0;
                }
            });

            this.getEntitiesCounts();
        }

        this.setSelectedBucket = function(bucket) {
            this.selectedBucket = bucket;
        }
        this.getSelectedBucket = function() {
            return this.selectedBucket;
        }

        this.setEntitiesProperty = function(property, value) {
            for (var key in this.counts) {
                this.counts[key][property] = value;
            }
        }

        this.setPublicProperty = function(property, value) {
            this.public[property] = value;
        }

        this.getPublicProperty = function(property) {
            return !this.public.hasOwnProperty(property) ? null : this.public[property];
        }

        this.getPublic = function() {
            return this.public;
        }

        this.setResourceTypeCount = function(resourceType, loading, value) {

            var resourceTypeCount = this.getCounts()[resourceType];

            if (resourceTypeCount) {
                if (typeof value !== 'undefined') {
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

        this.getRuleCount = function(bkt, bucketToRuleMap, bucketLabels, entity){
            // console.log(bkt, ' = ', entity);
            if (bkt) {
                var buckets = [
                    bucketToRuleMap[bkt.bucket] 
                ];
            } else {
                var buckets = [];

                bucketLabels.forEach(function(bucketName, index) {
                    buckets.push(bucketToRuleMap[bucketName]); 
                });
            }

            var accountRestrictions = [], contactRestrictions = [];
            var filteredAccounts = [], filteredContacts = [];
            buckets.forEach(function(bucket, index) {
                accountRestrictions = QueryStore.getAllBuckets(bucket['account_restriction'].logicalRestriction.restrictions);
                contactRestrictions = QueryStore.getAllBuckets(bucket['contact_restriction'].logicalRestriction.restrictions);

                filteredAccounts = filteredAccounts.concat(accountRestrictions.filter(function(value, index) {
                    return value.bucketRestriction && value.bucketRestriction.bkt && value.bucketRestriction.bkt.Id && !value.bucketRestriction.ignored;
                }));

                filteredContacts = filteredContacts.concat(contactRestrictions.filter(function(value, index) {
                    return value.bucketRestriction && value.bucketRestriction.bkt && value.bucketRestriction.bkt.Id && !value.bucketRestriction.ignored;
                }));
            });
            
            if (entity) {
                var counts = {
                            'account': filteredAccounts.length, 
                            'contact': filteredContacts.length
                        };
                return counts;
            }

            return filteredAccounts.length + filteredContacts.length;
        };

        this.setAccounts = function(query) {
            var deferred = $q.defer();

            this.GetDataByQuery('accounts', query).then(function(response) {
                QueryStore.accounts = response;
                deferred.resolve(response);
            });
            return deferred.promise;
        };

        this.getAccounts = function() {
            return this.accounts;
        };

        var self = this;
        this.validResourceTypes.forEach(function(resourceType) {
            self.setResourceTypeCount(resourceType, true);
        });

        this.setContacts = function(query) {
            var deferred = $q.defer();

            this.GetDataByQuery('contacts', query).then(function(response) {
                QueryStore.contacts = response;
                deferred.resolve(response);
            });
            return deferred.promise;
        };

        this.getContacts = function() {
            return this.contacts;
        };

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
                QueryStore.contactRestriction = contactRestriction;
            }
        };
        this.getContactRestriction = function() {
            return QueryStore.contactRestriction;
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
            this.setSegment(segment);
            this.resetRestrictions(segment);

            var aRS = JSON.stringify(segment ? segment.account_restriction : this.getAccountRestriction());
            var cRS = JSON.stringify(segment ? segment.contact_restriction : this.getContactRestriction());

            this.setDefaultRestrictions(aRS + cRS);
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

        this.setAccountBucketTreeRoot = function(tree) {
            this.accountBucketTreeRoot = tree;
        }

        this.setContactBucketTreeRoot = function(tree) {
            this.contactBucketTreeRoot = tree;
        }

        this.setAddBucketTreeRoot = function(tree, type) {
            if (tree === null) {
                delete this.addBucketTreeRoot;
            } else {
                this.addBucketTreeRoot = tree; // this causes PLS-6617
                this.addBucketTreeType = type ? type : '';
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

        function setBucketRestrictionUsed (bucketRestriction){
            // console.log('==================> Addded restriction ',QueryStore.mode, bucketRestriction.bkt);
            if(!bucketRestriction.bkt || bucketRestriction.bkt == {}){
                if((QueryStore.mode == 'rules' || QueryStore.mode == 'dashboardrules')){
                        bucketRestriction.ignored = true;
                }else{
                    bucketRestriction.ignored = bucketRestriction.ignored ? bucketRestriction.ignored : false;
                }
            }else{
                if((QueryStore.mode == 'rules' || QueryStore.mode == 'dashboardrules')){
                        bucketRestriction.ignored = true;
                }else{
                    bucketRestriction.ignored = bucketRestriction.ignored ? bucketRestriction.ignored : false;
                }
            } 

            // console.log('Addded restriction ',bucketRestriction);
        }

        this.addRestriction = function(type, attribute, forceTreeRoot) {
            
            attribute = this.setAttributeAttr(type, attribute);

            var treeRoot = this.getAddBucketTreeRoot(),
                restrictions = [],
                bucketRestriction = new BucketRestriction(
                    attribute.columnName,
                    attribute.resourceType,
                    (attribute.bkt ? attribute.bkt.Vals : null),
                    attribute.attr,
                    (attribute.bkt ? attribute.bkt : null)
                );

            setBucketRestrictionUsed(bucketRestriction);

            if (QueryStore.mode == 'rules' || QueryStore.mode == 'dashboardrules') {
                restrictions = this[type + 'BucketTreeRoot'].logicalRestriction.restrictions;
            } else if (forceTreeRoot || (treeRoot && type == this.addBucketTreeType)) {
                restrictions = forceTreeRoot ? forceTreeRoot : treeRoot.logicalRestriction.restrictions;
            } else {
                restrictions = this[type + 'Restriction'].restriction.logicalRestriction.restrictions;
                //console.log(':add:', type, attribute, bucketRestriction, restrictions);

                var sameAttributes = restrictions.filter(function(restriction) {
                    var br = restriction.bucketRestriction;
                    return br && br.attr == bucketRestriction.attr;
                });

                var logicalRestrictions = restrictions.filter(function(restriction) {
                    return restriction.logicalRestriction;
                });

                //console.log(':add:', sameAttributes, '\n', logicalRestrictions);

                var newHome = null;

                logicalRestrictions.forEach(function(logical) {
                    var rs = logical.logicalRestriction.restrictions;

                    var buckets = rs.filter(function(restriction) {
                        var br = restriction.bucketRestriction;
                        return br && br.attr == bucketRestriction.attr;
                    });

                    if (buckets.length > 0 && buckets.length == rs.length) {
                        newHome = rs;
                    }
                });

                if (newHome) {
                    restrictions = sameAttributes.length > 0 ? restrictions : newHome;
                }

                if (sameAttributes.length > 0) {
                    if (!newHome || (newHome && sameAttributes.length > 0)) {
                        var newLogicalRestriction = {
                            logicalRestriction: {
                                operator: 'OR',
                                restrictions: []
                            }
                        };

                        restrictions.push(newLogicalRestriction);

                        restrictions = newLogicalRestriction.logicalRestriction.restrictions;
                    }

                    sameAttributes.forEach(function(restriction) {
                        restriction.bucketRestriction.columnName = attribute.columnName;
                        restriction.bucketRestriction.resourceType = attribute.resourceType;

                        QueryStore.addRestriction(type, restriction.bucketRestriction, restrictions);
                        QueryStore.removeRestriction(type, restriction.bucketRestriction);
                    });
                }
            }

            restrictions.push({
                bucketRestriction: bucketRestriction
            });

            this.setRestrictions(type);
        }

        this.removeRestriction = function(type, attribute) {
            //console.log(':remove:', type, attribute);

            attribute = this.setAttributeAttr(type, attribute);
            //console.log(':remove attr:', type, attribute);

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
            type == 'account' ?
                this.setAccountRestriction(this.accountRestriction) :
                this.setContactRestriction(this.contactRestriction);

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
            return {
                groupKey: groupKey,
                attributes: attributes
            };
        };

        this.findAttributesInGroup = function(groupKey, columnName) {
            var group = this.restriction[groupKey];

            var results = [];

            for (var i = 0; i < group.length; i++) {
                //console.log(group, "fired");
                if (group[i].bucketRestriction.columnName === columnName) {
                    results.push({
                        index: i,
                        bucketRestriction: group[i].bucketRestriction
                    });
                }
            }
            return results;
        };

        this.getCollectionStatus = function() {
            var deferred = $q.defer();
            if (QueryStore.collectionStatus != null) {
                deferred.resolve(QueryStore.collectionStatus);
            } else {
                QueryService.GetCollectionStatus().then(function(response){
                    QueryStore.collectionStatus = response;
                    QueryStore.isDataAvailable = response && (response.AccountCount != 0 || response.ContactCount != 0);
                    deferred.resolve(response);
                });
            }
            return deferred.promise;
        }

        this.getEntitiesCounts = function(query) {
            var deferred = $q.defer();

            this.GetEntitiesCountsByQuery(query).then(function(data) {
                if (!data) {
                    deferred.resolve({});
                } else {
                    QueryStore.setResourceTypeCount('accounts', false, data['Account']);
                    QueryStore.setResourceTypeCount('contacts', false, data['Contact']);
                    QueryStore.setEntitiesProperty('loading', false);

                    deferred.resolve(data);
                }
            });

            return deferred.promise;
        };

        this.GetEntitiesCountsByQuery = function(query) {
            var deferred = $q.defer(),
                accountRestriction = this.getAccountRestriction(),
                contactRestriction = this.getContactRestriction();

            if (query === undefined || query === '') {
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
                        errMsg: 'Invalid resourceType: ' + resourceType
                    }
                });

                return deferred.promise;
            } else {
                var deferred = $q.defer(),
                    accountRestriction = this.getAccountRestriction(),
                    contactRestriction = this.getContactRestriction();

                if (query === undefined || query === '') {
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
                    //console.log('Resource Type', resourceType);
                    deferred.resolve(data[resourceType == 'account' ? 'Account' : 'Contact']);
                });

                return deferred.promise;
            }
        };

        this.GetDataByQuery = function(resourceType, query) {
            // console.log(query);
            if (!this.isValidResourceType(resourceType)) {
                var deferred = $q.defer();
                deferred.resolve({
                    error: {
                        errMsg: 'Invalid resourceType: ' + resourceType
                    }
                });
                return deferred.promise;
            } else {

                var deferred = $q.defer();

                var queryWithRestriction = {
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
                if(resourceType === 'accounts') {
                    queryWithRestriction.lookups = query.lookups;
                }
                if(query.sort){
                    queryWithRestriction['sort'] = query.sort;
                }

                // console.log(queryWithRestriction);
                    
                deferred.resolve(QueryService.GetDataByQuery(resourceType, queryWithRestriction));
                return deferred.promise;
            }
        };

        this.isValidResourceType = function(resourceType) {
            //console.log('isValidResourceType', resourceType);
            return this.validResourceTypes.indexOf(resourceType) > -1;
        };

        this.getAllBuckets = function(tree, restrictions, getEmptyBuckets) {
            restrictions = restrictions || [];

            tree.forEach(function(branch) {
                if (branch && branch.bucketRestriction && branch.bucketRestriction) {
                    if (getEmptyBuckets || (branch.bucketRestriction.bkt && typeof branch.bucketRestriction.bkt.Id == 'number')) {
                        restrictions.push(branch);
                    }
                }

                if (branch && branch.logicalRestriction) {
                    QueryStore.getAllBuckets(branch.logicalRestriction.restrictions, restrictions);
                }
            });

            return restrictions;
        };

        this.getDataCloudAttributes = function(ignoreCache, getEmptyBuckets) {
            //var treeRoot = QueryStore.getAddBucketTreeRoot();
            var restrictions = [];

            getEmptyBuckets = getEmptyBuckets || false;

            if (QueryStore.mode == 'rules' || QueryStore.mode == 'dashboardrules') {
                this.getAllBuckets(QueryStore.accountBucketTreeRoot.logicalRestriction.restrictions, restrictions, getEmptyBuckets);
                this.getAllBuckets(QueryStore.contactBucketTreeRoot.logicalRestriction.restrictions, restrictions, getEmptyBuckets);
                // this.getAllBuckets(this.treeRoot.logicalRestriction.restrictions, restrictions);
            } else {
                var ar = QueryStore.getAccountRestriction();
                var cr = QueryStore.getContactRestriction();

                this.getAllBuckets(ar.restriction.logicalRestriction.restrictions, restrictions);
                this.getAllBuckets(cr.restriction.logicalRestriction.restrictions, restrictions);
            }

            return restrictions;
        }

        this.generateBucketLabel = function(bkt) {
            var a, b;

            if (bkt.Vals) {
                var format = NumberUtility.AbbreviateLargeNumber;
                var abbrs = ["K", "M", "B"]; // no T from Backend?

                a = format(bkt.Vals[0], 0, abbrs);
                b = format(bkt.Vals[1], 0, abbrs);
            }

            switch (bkt.Cmp) {
                case 'Yes':
                    bkt.Lbl = 'Yes';
                    break;
                case 'empty':
                    bkt.Lbl = '';
                    break;
                case 'EQUAL':
                    bkt.Lbl = bkt.Vals[0];
                    break;
                case 'IS_NULL':
                    bkt.Lbl = '';
                    break;
                case 'IS_NOT_NULL':
                    bkt.Lbl = '';
                    break;
                case 'GREATER_THAN':
                    bkt.Lbl = '> ' + a;
                    break;
                case 'LESS_THAN':
                    bkt.Lbl = '< ' + a;
                    break;
                case 'GREATER_OR_EQUAL':
                    bkt.Lbl = '>= ' + a;
                    break;
                case 'LESS_OR_EQUAL':
                    bkt.Lbl = '<= ' + a;
                    break;
                case 'GT_AND_LTE':
                    bkt.Lbl = '> ' + a + ' and <= ' + b;
                    break;
                case 'GT_AND_LT':
                    bkt.Lbl = '> ' + a + ' and < ' + b;
                    break;
                case 'GTE_AND_LTE':
                    bkt.Lbl = '>= ' + a + ' and <= ' + b;
                    break;
                case 'GTE_AND_LT':
                    bkt.Lbl = a + ' - ' + b;
                    break;
                case 'between':
                    bkt.Lbl = a + ' - ' + b;
                    break;
                case 'BETWEEN':
                    bkt.Lbl = a + ' - ' + b;
                    break;
                case 'IN_COLLECTION':
                    bkt.Lbl = bkt.Vals.join(', ');
                    break;
                case 'NOT_IN_COLLECTION':
                    bkt.Lbl = 'not ' + bkt.Vals.join(', ');
                    break;
                default:
                    if (bkt.Cmp) {
                        bkt.Lbl = (bkt.Vals && bkt.Vals.length > 0) ? a : 'empty';
                    }
            }

            return bkt;
        }

        this.init();
    })
    .service('QueryService', function($http, $q, SegmentStore) {
        this.canceler = null;
        this.cancelerCounts = null;

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

    this.GetCollectionStatus = function() {
        var deferred = $q.defer(),
            url = '/pls/datacollection/status';
        
        $http({
            method: 'get',
            url: url
        }).then(function(response){
            deferred.resolve(response.data);
        }, function(response) {
            deferred.resolve({});
        });
        return deferred.promise;
    };

    this.GetEntitiesCounts = function(query, cancelPrevious) {
        var deferred = $q.defer();

        if (this.cancelerCounts) {
            this.cancelerCounts.resolve("cancelled");
        }

        this.cancelerCounts = $q.defer();

        SegmentStore.sanitizeSegment(query);

        $http({
            method: 'POST',
            url: '/pls/entities/counts',
            data: query,
            timeout: this.cancelerCounts.promise,
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