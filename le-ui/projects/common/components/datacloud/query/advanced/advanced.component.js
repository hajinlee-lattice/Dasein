angular.module('common.datacloud.query.advanced', [
    'common.datacloud.query.advanced.input',
    'common.datacloud.query.advanced.tree'
])
.controller('AdvancedQueryCtrl', function($scope, $state, $stateParams, 
    QueryRestriction, QueryStore, DataCloudStore, SegmentServiceProxy, 
    BucketRestriction, CurrentConfiguration, BrowserStorageUtility, QueryStore
) {
    var vm = this;

    angular.extend(this, {
        inModel: $state.current.name.split('.')[1] === 'model',
        items: {
            '1': {
                label: '1',
                ColumnId: 'poweredge_t330_tower_server_has_purchased',
                Category: 'Product',
                Subcategory: 'PowerEdge T330 Tower Server',
                DisplayName: 'Has Purchased',
                Value: 'Yes',
                Operator: 'equals',
                RecordsCount: 7982
            },
            '2': {
                label: '2',
                ColumnId: 'poweredge_t330_tower_server_spent',
                Category: 'Product',
                Subcategory: 'PowerEdge T330 Tower Server',
                DisplayName: 'Has Spent',
                Value: '$10K - $50K',
                Operator: 'equals',
                RecordsCount: 712
            },
            '3': {
                label: '3',
                ColumnId: 'poweredge_t330_tower_server_spent',
                Category: 'Product',
                Subcategory: 'PowerEdge T330 Tower Server',
                DisplayName: 'Has Spent',
                Value: '$50K - $100K',
                Operator: 'equals',
                RecordsCount: 471
            },
            '4': {
                label: '4',
                ColumnId: 'firmographics_finance',
                Category: 'Firmographics',
                Subcategory: 'Other',
                DisplayName: 'Finance',
                Value: 'Yes',
                Operator: 'equals',
                RecordsCount: 3231
            },
            '5': {
                label: '5',
                ColumnId: 'poweredge_t330_tower_server_spent',
                Category: 'Product',
                Subcategory: 'PowerEdge T330 Tower Server',
                DisplayName: 'Has Spent',
                Value: '$100K - $200K',
                Operator: 'equals',
                RecordsCount: 7374
            },
            '6': {
                label: '6',
                ColumnId: 'intent_multi-currency_accounting',
                Category: 'Intent',
                Subcategory: 'Other',
                DisplayName: 'Multi-Currency Accounting',
                Value: 'No',
                Operator: 'equals',
                RecordsCount: 8476
            },
            '7': {
                label: '7',
                ColumnId: 'technology_profile_has_3scale',
                Category: 'Technology Profile',
                Subcategory: 'Application Development & Management',
                DisplayName: 'Has 3Scale',
                Value: 'Yes',
                Operator: 'equals',
                RecordsCount: 5657
            },
            '8': {
                label: '8',
                ColumnId: 'contact_department',
                Category: 'Contact',
                Subcategory: 'Other',
                DisplayName: 'Department',
                Value: 'IT',
                Operator: 'equals',
                RecordsCount: 843
            },
            '9': {
                label: '9',
                ColumnId: 'growth_trends_credit_risk_rank',
                Category: 'Growth Trends',
                Subcategory: 'Other',
                DisplayName: 'Credit Risk Rank',
                Value: '1-Promote',
                Operator: 'equals',
                RecordsCount: 4765
            },
            '10': {
                label: '10',
                ColumnId: 'growth_trends_credit_risk_rank',
                Category: 'Growth Trends',
                Subcategory: 'Other',
                DisplayName: 'Credit Risk Rank',
                Value: '2-Viable',
                Operator: 'equals',
                RecordsCount: 8233
            },
            '11': {
                label: '11',
                ColumnId: 'growth_trends_credit_risk_rank',
                Category: 'Growth Trends',
                Subcategory: 'Other',
                DisplayName: 'Credit Risk Rank',
                Value: '5-Avoid',
                Operator: 'equals',
                RecordsCount: 56565
            },
            '12': {
                label: '12',
                ColumnId: 'website_profile_credit_risk_rank',
                Category: 'Website Profile',
                Subcategory: 'Abandonment Ads',
                DisplayName: 'Has Realtime Targeting',
                Value: 'Yes',
                Operator: 'equals',
                RecordsCount: 2476
            }
        },
        tree3: { 
            "accounts": {
                name: 'accounts',
                type: 'and',
                items: [{ 
                        type: 'and',
                        item: '1' 
                    },{ 
                        type: 'or',
                        items: [{ 
                                type: 'or',
                                item: '2' 
                            },{ 
                                type: 'or',
                                item: '3'  
                            }
                        ]
                    },{ 
                        type: 'and',
                        item: '4' 
                    }
                ]
            }
        },
        tree2: { 
            "accounts": {
                name: 'accounts',
                type: 'and',
                items: [{ 
                        type: 'and',
                        item: '1' 
                    },{ 
                        type: 'and',
                        item: '4' 
                    },{ 
                        type: 'or',
                        items: [{ 
                                type: 'or',
                                item: '2' 
                            },{ 
                                type: 'or',
                                item: '3'  
                            }
                        ]
                    }
                ]
            }
        },
        tree1: { 
            "accounts": {
                name: 'accounts',
                type: 'and',
                items: [{ 
                        type: 'and',
                        item: '4' 
                    },{ 
                        type: 'and',
                        item: '11' 
                    },{
                        type: 'or',
                        items: [{ 
                                type: 'or',
                                item: '2' 
                            },{ 
                                type: 'or',
                                item: '3' 
                            },{ 
                                type: 'or',
                                item: '5' 
                            },{
                                type: 'and',
                                items: [{ 
                                        type: 'and',
                                        item: '9' 
                                    },{ 
                                        type: 'or',
                                        items: [{ 
                                                type: 'or',
                                                item: '7' 
                                            },{ 
                                                type: 'or',
                                                item: '8'
                                            }
                                        ]
                                    },{ 
                                        type: 'and',
                                        item: '10' 
                                    }
                                ]
                            },{ 
                                type: 'or',
                                item: '6' 
                            }
                        ]
                    },{ 
                        type: 'and',
                        item: '12' 
                    },{ 
                        type: 'or',
                        items: [{ 
                                type: 'and',
                                items: [{ 
                                        type: 'and',
                                        item: '8' 
                                    },{ 
                                        type: 'and',
                                        item: '11'
                                    }
                                ]
                            },{
                                type: 'or',
                                item: '7' 
                            } 
                        ]
                    },{ 
                        type: 'and',
                        item: '1' 
                    }
                ]
            }
        },
        state: $stateParams.state,
        operator: 'and',
        enrichments: [],
        restriction: QueryStore.restriction
    });

    vm.init = function () {
        console.log('QueryStore',QueryStore);
        vm.buildFlatAttributesArray();
    };

    vm.buildFlatAttributesArray = function() {
console.log(vm.restriction.restriction.logicalRestriction.restrictions[0].logicalRestriction.restrictions)
    }

    vm.click = function () {
        vm.state = vm.state == 'tree1' 
        ? 'tree2' 
        : vm.state == 'tree2' 
            ?'tree3' 
            :'tree1';
        

        var parms = angular.extend({}, $stateParams, { state: vm.state });

        $state.transitionTo($state.current, parms, { 
          notify: true
        });
    };

    vm.goAttributes = function() {
        var state = vm.inModel
                ? 'home.model.analysis.explorer.attributes'
                : 'home.segment.explorer.attributes';

        $state.go(state);
    };

    vm.init();
});
