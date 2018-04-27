angular.module('common.datacloud.query')
.config(function($stateProvider) {
    var resolveGetData = function(type) {
        return [
            '$q', '$stateParams', 'QueryStore', 'SegmentStore', 
            function($q, $stateParams, QueryStore, SegmentStore) {
                var deferred = $q.defer(),
                    name = $stateParams.segment,
                    getQuery = function(name, account, contact) {
                        return { 
                            free_form_text_search: '',
                            account_restriction: account || store.getAccountRestriction,
                            contact_restriction: contact,
                            preexisting_segment_name: name,
                            page_filter: {
                                num_rows: 10,
                                row_offset: 0
                            }
                        };
                    };

                if (name === "Create") {
                    var account_restriction = QueryStore.getAccountRestriction(),
                        contact_restriction = QueryStore.getContactRestriction(),
                        query = getQuery(name, account_restriction, contact_restriction),
                        result = QueryStore.GetDataByQuery(type, query).then(function(data) { 
                            return data; 
                        });

                    deferred.resolve(result);
                } else {
                    SegmentStore.getSegmentByName(name).then(function(segment) {
                        var query = getQuery(name, segment.account_restriction, segment.contact_restriction),
                            result = QueryStore.GetDataByQuery(type, query).then(function(data) { 
                                return data; 
                            });

                        deferred.resolve(result);
                    });
                };

                return deferred.promise;
            }
        ];
    };

    $stateProvider
        .state('home.segment.explorer.builder', {
            url: '/builder',
            params: {
                pageIcon: 'ico-analysis',
                pageTitle: 'Query Builder'
            },
            resolve: {
                Cube: ['$q', 'DataCloudStore', function($q, DataCloudStore){
                    var deferred = $q.defer();

                    DataCloudStore.getCube().then(function(result) {
                        if (result.data) {
                            deferred.resolve(result.data);
                        }
                    });
                    
                    return deferred.promise;
                }],
                RatingEngineModel: [function() {
                    return null;
                }],
                CurrentRatingEngine: [function() {
                    return null;
                }],
            },
            views: {
                "main@": {
                    controller: 'AdvancedQueryCtrl',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/query/advanced/advanced.component.html'
                }
            }
        })
        .state('home.segment.accounts', {
            url: '/accounts',
            params: {
                pageIcon: 'ico-analysis',
                pageTitle: 'Accounts'
            },
            resolve: {
                Accounts: resolveGetData('accounts'),
                Contacts: [function() { return null; }],
                // for the Playbook wizard Targets tab
                NoSFIdsCount: [function() { return null; }],
                AccountsCoverage: [function() { return null; }],
                Config: [function() { return null; }],
            },
            views: {
                "main@": {
                    controller: 'QueryResultsCtrl',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/query/results/queryresults.component.html'
                }
            }
        })
        .state('home.segment.contacts', {
            url: '/contacts',
            params: {
                pageIcon: 'ico-analysis',
                pageTitle: 'Contacts'
            },
            resolve: {
                Contacts: resolveGetData('contacts'),
                Accounts: [function() { return null; }],
                // for the Playbook wizard Targets tab
                NoSFIdsCount: [function() { return null; }],
                AccountsCoverage: [function() { return null; }],
                Config: [function() { return null; }],
            },
            views: {
                "main@": {
                    controller: 'QueryResultsCtrl',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/query/results/queryresults.component.html'
                }
            }
        });
});
