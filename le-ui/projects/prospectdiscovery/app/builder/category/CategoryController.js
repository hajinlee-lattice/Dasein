angular
    .module('pd.builder.attributes', [
        'pd.navigation.pagination'
    ])
    .service('AttributesModel', function($q, AttributesService, $filter) {
        var AttributesModel = this;

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

                AttributesService.get(args).then(function(list) {
                    list = list || [];

                    list.forEach(function(item, index) {
                        // FIXME - Make REGION top level, because Country data is limited
                        if (item.ParentKey == 'Country')
                            item.ParentKey = '_OBJECT_';

                        // FIXME - Fudging the numbers a bit for the demo
                        // Will be removing this stuff when API returns real data
                        item.lift = (Math.random() * 3.0).toFixed(1) + 'x';
                        item.revenue = Math.round(Math.random() * 30000000);
                        item.lattice_companies = item.Properties.CompanyCount;
                        item.their_companies = Math.round(Math.random() * item.lattice_companies);
                        item.customers = Math.round(Math.random() * item.their_companies);
                        item.their_companies = item.their_companies > item.lattice_companies ? item.lattice_companies >> 1 : item.their_companies;
                        item.customers = item.customers > item.their_companies ? item.their_companies >> 1 : item.customers
                        
                        // Calculate percentages for bubble visualization
                        item.companies_percent = ((item.their_companies / item.lattice_companies) * 100).toFixed(3);
                        item.customer_percent = ((item.customers / item.lattice_companies) * 100).toFixed(3);
                        item.customer_percent_of_theirs = ((item.customers / item.their_companies) * 100).toFixed(3);
                        
                        // set a minimum constaint just so things look good
                        item.companies_percent = item.companies_percent < 10 ? 10 : item.companies_percent;
                        item.customer_percent = item.customer_percent < 5 ? 5 : item.customer_percent;

                        item.selected = false;

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
    .controller('AttributesCtrl', function($state, $stateParams, AttributesModel) {
        this.SubCategoryMap = AttributesModel.SubCategoryMap;
        this.ParentCategoryMap = AttributesModel.ParentCategoryMap;

        angular.extend(this, $stateParams, {
            init: function() {
                this.SortProperty = 'AttrValue';
                this.SortDirection = '';
                this.SearchValue = '';
                this.ShowSearch = false;
                this.FilterChecked = false;
                this.TruncateLimit = 32;

                if (!this.AttrKey) {
                    return console.log('<!> No stateParams provided');
                }

                // This might work better in a UI-Router "resolve"
                AttributesModel
                    .getList($stateParams)
                    .then(angular.bind(this, this.processList));
            },

            processList: function(list) {
                this.total = list.length;
                this.list = list;
                this.SubCategory = this.SubCategoryMap[this.AttrKey];
                this.ParentCategory = this.ParentCategoryMap[this.AttrKey] || this.AttrKey;
            },

            // Interaction with the Filters Sorting drop-down
            handleSortPropertyChange: function($event) { },

            // Drill down to sub category if one exists
            handleTileClick: function($event, item) {
                if (this.SubCategory) {
                    $state.go("builder.category", { 
                        ParentKey: this.ParentCategory, 
                        ParentValue: item.AttrValue, 
                        AttrKey: this.SubCategory
                    });
                }
            },

            // Parent selects all children, and a child makes sure parent is selected.
            handleTileSelect: function(targetTile) {
                var SubCategory = this.SubCategory;

                targetTile.modified = Date.now();
                targetTile.visible = !SubCategory;

                if (SubCategory) {
                    AttributesModel
                        .getList({
                            ParentKey: this.ParentCategory,
                            ParentValue: targetTile.AttrValue,
                            AttrKey: SubCategory
                        })
                        .then(angular.bind(this, function(result) {
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
                        .then(angular.bind(this, function(result) {
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
                            .then(angular.bind(this, function(result) {
                                (result.length > 0 ? result[0] : {})
                                    .selected = targetTile.selected;
                            }));
                    }
                }
            }
        });

        this.init();
    })