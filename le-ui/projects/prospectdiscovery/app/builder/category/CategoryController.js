angular
    .module('pd.builder.attributes', [
        'pd.navigation.pagination'
    ])
    .service('AttributesModel', function($q, AttributesService, StateMapping, $filter) {
        // Map used to determine AttrKey's sub category
        this.SubCategoryMap = {
            'Industry': 'SubIndustry',
            'Locations': 'State'
        };

        // Map converts friendly labels to ParentKey
        this.ParentCategoryMap = {
            'Locations': 'Region',
            'Employees Range': 'EmployeesRange',
            'Revenue Range': 'RevenueRange'
        };

        this.MasterList = [];

        // Filters MasterList via args (ParentKey/ParentValue/AttrKey/AttrValue)
        this.getMaster = function(args) {
            var fields = {
                AttrKey: (this.ParentCategoryMap[args.AttrKey] || args.AttrKey)
            };

            // only pass through args that have a definite value and in whitelist
            ['AttrValue','ParentValue','ParentKey','selected','visible'].forEach(function(key) {
                args[key] ? fields[key] = args[key] : null;
            });

            var list = $filter('filter')(this.MasterList, fields, true);

            return list || [];
        }

        // checks if items matching args exists, performs XHR to fetch if they don't
        this.getList = function(args) {
            var deferred = $q.defer(),
                master = this.getMaster(args);

            if (master && master.length > 0) {
                setTimeout(function() {
                    deferred.resolve(master);
                }, 1);
            } else {
                master = this.MasterList;
                
                var get = function(item, key, random_max) {
                    return item.Properties
                        ? item.Properties[key] && item.Properties[key] != "0"
                            ? item.Properties[key]
                            : Math.round(Math.random() * random_max)
                        : 'null';
                };

                AttributesService.get(args).then(function(list) {
                    console.log('xhr result',list);
                    list = list || [];

                    list.forEach(function(item, index) {

                        // FIXME - Make REGION top level, because Country data is limited
                        if (item.ParentKey == 'Country') {
                            item.ParentKey = '_OBJECT_';
                        }

                        item.selected = false;
                        item.AttrValue = StateMapping[item.AttrValue] || item.AttrValue;

                        // FIXME - Fudging the numbers a bit for the demo
                        // Will be removing this stuff when API returns real data
                        item.lift = get(item, 'lift', 3.0);
                        item.total = get(item, 'SubCategoryCount', 15);
                        item.revenue = get(item, 'revenue', 30000000);
                        item.lattice_companies = get(item, 'CompanyCount', 99999);
                        item.their_companies = get(item, 'in_your_db_count', item.lattice_companies);
                        item.customers = get(item, 'your_customer_count', item.their_companies);
                   
                        // Calculate percentages for bubble visualization
                        item.companies_percent = ((item.their_companies / item.lattice_companies) * 100);
                        item.customer_percent = ((item.customers / item.lattice_companies) * 100);
                        item.customer_percent_of_theirs = ((item.customers / item.their_companies) * 100);

                        master.push(item);
                    });

                    deferred.resolve(list);
                });
            }

            return deferred.promise;
        }
    })
    .service('AttributesService', function($http, $q) {
        this.QueryMap = {
            'Industry': [ 'Industry' ],
            'Locations': [ 'Region' ],
            'Employees Range': [ 'EmployeesRange' ],
            'Revenue Range': [ 'RevenueRange' ]
        };

        this.get = function(args) {
            var deferred = $q.defer();
            var queries = [];
            var components = this.QueryMap[args.AttrKey];

            (components || [ args.AttrKey ]).forEach(function(AttrKey, index) {
                query = {
                    AttrKey: AttrKey
                };

                if (args.ParentKey) {
                    query.ParentKey = args.ParentKey;
                    query.ParentValue = args.ParentValue;
                }
                
                queries.push(query);
            });
            
            var url = '/pls/amattributes' +
                      '?populate=true' +
                      '&queries=' + encodeURIComponent(JSON.stringify(queries));

            $http({
                method: 'GET',
                url: url
            }).then(
                function onSuccess(response) {
                    var result = [];

                    for (var i=0; i<response.data.length; i++) {
                        result = result.concat(response.data[i]);
                    }

                    deferred.resolve(result);
                }, function onError(response) {
                    deferred.reject('Error fetching data.')
                }
            );
            
            return deferred.promise;
        };
    })
    .controller('AttributesCtrl', function($scope, $state, $stateParams, AttributesModel) {
        var vm = this;

        vm.SubCategoryMap = AttributesModel.SubCategoryMap;
        vm.ParentCategoryMap = AttributesModel.ParentCategoryMap;

        angular.extend(vm, $stateParams, {
            init: function() {
                vm.SortProperty = 'AttrValue';
                vm.SortDirection = '';
                vm.SearchValue = '';
                vm.ShowSearch = false;
                vm.FilterChecked = false;
                vm.TruncateLimit = 32;

                if (!vm.AttrKey) {
                    return console.log('<!> No stateParams provided');
                }

                // This might work better in a UI-Router "resolve"
                AttributesModel
                    .getList({ 
                        AttrKey: vm.AttrKey,
                        AttrValue: vm.AttrValue,
                        ParentKey: vm.ParentKey,
                        ParentValue: vm.ParentValue
                    })
                    .then(angular.bind(vm, vm.processList));
            },

            processList: function(list) {
                vm.total = list.length;
                vm.list = list;
                vm.SubCategory = vm.SubCategoryMap[vm.AttrKey];
                vm.ParentCategory = vm.ParentCategoryMap[vm.AttrKey] || vm.AttrKey;
            },

            // Interaction with the Filters Sorting drop-down
            handleSortPropertyChange: function($event) { },

            // Drill down to sub category if one exists
            handleTileClick: function($event, item) {
                if (vm.SubCategory) {
                    $state.go("builder.category", { 
                        ParentKey: vm.ParentCategory, 
                        ParentValue: item.AttrValue, 
                        AttrKey: vm.SubCategory
                    });
                }
            },

            // Parent selects all children, and a child makes sure parent is selected.
            handleTileSelect: function(targetTile) {
                var SubCategory = vm.SubCategory;

                targetTile.modified = Date.now();
                targetTile.visible = !SubCategory;

                if (SubCategory) {
                    AttributesModel
                        .getList({
                            ParentKey: vm.ParentCategory,
                            ParentValue: targetTile.AttrValue,
                            AttrKey: SubCategory
                        })
                        .then(angular.bind(vm, function(result) {
                            targetTile.total = result.length;

                            result.forEach(function(item, index) {
                                item.selected = targetTile.selected;
                                item.modified = Date.now();
                                item.visible = true;
                            });
                        }));
                } else if (targetTile.selected) {
                    AttributesModel
                        .getList({
                            AttrKey: targetTile.ParentKey,
                            AttrValue: targetTile.ParentValue
                        })
                        .then(angular.bind(vm, function(result) {
                            (result.length > 0 ? result[0] : {})
                                .selected = targetTile.selected;
                        }));
                } else {
                    var SelectedSiblings = AttributesModel.getMaster({
                        ParentValue: targetTile.ParentValue,
                        ParentKey: targetTile.ParentKey,
                        selected: true
                    });

                    if (SelectedSiblings.length == 0) {
                        AttributesModel
                            .getList({
                                AttrKey: targetTile.ParentKey,
                                AttrValue: targetTile.ParentValue
                            })
                            .then(angular.bind(vm, function(result) {
                                (result.length > 0 ? result[0] : {})
                                    .selected = targetTile.selected;
                            }));
                    }
                }
            }
        });
        /*
        $scope.$on('$stateChangeStart', function(event, toState, toParams, fromState, fromParams){
            console.log('preventing transition', $state.current.name, event,'\n', toState,'\n', toParams, '\n', fromState, '\n', fromParams);
            
            if ($state.current.name == 'builder.category') {
                angular.extend(vm, toParams);
                vm.init();
                event.preventDefault(); 
            }
        });
        */
        vm.init();

        //$("span.subcategory-header-controls .custom-select").selectpicker();
    })
    .service('StateMapping', function() {
        /* 
            FIXME: there are no collisions yet, but this should be in some
                   kind of tree structure, but backend might have data soon
        */
        return {
            // usa states
            "AL": "Alabama",
            "AK": "Alaska",
            "AS": "American Samoa",
            "AZ": "Arizona",
            "AR": "Arkansas",
            "CA": "California",
            "CO": "Colorado",
            "CT": "Connecticut",
            "DE": "Delaware",
            "DC": "District Of Columbia",
            "FM": "Federated States Of Micronesia",
            "FL": "Florida",
            "GA": "Georgia",
            "GU": "Guam",
            "HI": "Hawaii",
            "ID": "Idaho",
            "IL": "Illinois",
            "IN": "Indiana",
            "IA": "Iowa",
            "KS": "Kansas",
            "KY": "Kentucky",
            "LA": "Louisiana",
            "ME": "Maine",
            "MH": "Marshall Islands",
            "MD": "Maryland",
            "MA": "Massachusetts",
            "MI": "Michigan",
            "MN": "Minnesota",
            "MS": "Mississippi",
            "MO": "Missouri",
            "MT": "Montana",
            "NE": "Nebraska",
            "NV": "Nevada",
            "NH": "New Hampshire",
            "NJ": "New Jersey",
            "NM": "New Mexico",
            "NY": "New York",
            "NC": "North Carolina",
            "ND": "North Dakota",
            "MP": "Northern Mariana Islands",
            "OH": "Ohio",
            "OK": "Oklahoma",
            "OR": "Oregon",
            "PW": "Palau",
            "PA": "Pennsylvania",
            "PR": "Puerto Rico",
            "RI": "Rhode Island",
            "SC": "South Carolina",
            "SD": "South Dakota",
            "TN": "Tennessee",
            "TX": "Texas",
            "UT": "Utah",
            "VT": "Vermont",
            "VI": "Virgin Islands",
            "VA": "Virginia",
            "WA": "Washington",
            "WV": "West Virginia",
            "WI": "Wisconsin",
            "WY": "Wyoming",
            // canada provinces
            "AB": "Alberta",
            "BC": "British Columbia",
            "MB": "Manitoba",
            "NB": "New Brunswick",
            "NL": "Newfoundland and Labrador",
            "NS": "Nova Scotia",
            "ON": "Ontario",
            "PE": "Prince Edward Island",
            "QC": "Quebec",
            "SK": "Saskatchewan",
            // canada territories
            "NT": "Northwest Territories",
            "NU": "Nunavut",
            "YT": "Yukon"
        };
    });