angular.module('common.datacloud.query.advanced', [
    'common.datacloud.query.advanced.input',
    'common.datacloud.query.advanced.tree'
])
.controller('AdvancedQueryCtrl', function($scope, $state, $stateParams, 
    QueryRestriction, QueryStore, DataCloudStore, SegmentServiceProxy, 
    BucketRestriction, CurrentConfiguration, BrowserStorageUtility
) {
    var vm = this;

    angular.extend(this, {
        inModel: $state.current.name.split('.')[1] === 'model',
        items: {
            '1': {
                label: '1',
                fieldname: 'poweredge_t330_tower_server_has_purchased',
                category: 'Product',
                subcategory: 'PowerEdge T330 Tower Server',
                displayname: 'Has Purchased',
                value: 'Yes',
                operator: 'equals',
                records: 7982
            },
            '2': {
                label: '2',
                fieldname: 'poweredge_t330_tower_server_spent',
                category: 'Product',
                subcategory: 'PowerEdge T330 Tower Server',
                displayname: 'Has Spent',
                value: '$10K - $50K',
                operator: 'equals',
                records: 712
            },
            '3': {
                label: '3',
                fieldname: 'poweredge_t330_tower_server_spent',
                category: 'Product',
                subcategory: 'PowerEdge T330 Tower Server',
                displayname: 'Has Spent',
                value: '$50K - $100K',
                operator: 'equals',
                records: 471
            },
            '4': {
                label: '4',
                fieldname: 'firmographics_finance',
                category: 'Firmographics',
                subcategory: 'Other',
                displayname: 'Finance',
                value: 'Yes',
                operator: 'equals',
                records: 3231
            },
            '5': {
                label: '5',
                fieldname: 'poweredge_t330_tower_server_spent',
                category: 'Product',
                subcategory: 'PowerEdge T330 Tower Server',
                displayname: 'Has Spent',
                value: '$100K - $200K',
                operator: 'equals',
                records: 7374
            },
            '6': {
                label: '6',
                fieldname: 'intent_multi-currency_accounting',
                category: 'Intent',
                subcategory: 'Other',
                displayname: 'Multi-Currency Accounting',
                value: 'No',
                operator: 'equals',
                records: 8476
            },
            '7': {
                label: '7',
                fieldname: 'technology_profile_has_3scale',
                category: 'Technology Profile',
                subcategory: 'Application Development & Management',
                displayname: 'Has 3Scale',
                value: 'Yes',
                operator: 'equals',
                records: 5657
            },
            '8': {
                label: '8',
                fieldname: 'contact_department',
                category: 'Contact',
                subcategory: 'Other',
                displayname: 'Department',
                value: 'IT',
                operator: 'equals',
                records: 843
            },
            '9': {
                label: '9',
                fieldname: 'growth_trends_credit_risk_rank',
                category: 'Growth Trends',
                subcategory: 'Other',
                displayname: 'Credit Risk Rank',
                value: '1-Promote',
                operator: 'equals',
                records: 4765
            },
            '10': {
                label: '10',
                fieldname: 'growth_trends_credit_risk_rank',
                category: 'Growth Trends',
                subcategory: 'Other',
                displayname: 'Credit Risk Rank',
                value: '2-Viable',
                operator: 'equals',
                records: 8233
            },
            '11': {
                label: '11',
                fieldname: 'growth_trends_credit_risk_rank',
                category: 'Growth Trends',
                subcategory: 'Other',
                displayname: 'Credit Risk Rank',
                value: '5-Avoid',
                operator: 'equals',
                records: 56565
            },
            '12': {
                label: '12',
                fieldname: 'website_profile_credit_risk_rank',
                category: 'Website Profile',
                subcategory: 'Abandonment Ads',
                displayname: 'Has Realtime Targeting',
                value: 'Yes',
                operator: 'equals',
                records: 2476
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
        operator: 'and'
    });

    vm.init = function () {
    };

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
