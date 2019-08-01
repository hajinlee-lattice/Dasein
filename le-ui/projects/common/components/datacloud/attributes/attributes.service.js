
angular.module('common.attributes')
    .service('AttrConfigStore', function (
        $q, $state, $stateParams, $timeout, AttrConfigService,
        DataCloudStore, BrowserStorageUtility, Modal
    ) {
        var store = this;

        this.init = function () {
            this.filters = {
                page: 1,
                pagesize: 25,
                sortPrefix: '+',
                queryText: '',
                showFilterBy: false,
                show: {
                    Selected: false,
                    IsPremium: false
                },
                hide: {
                    Selected: false,
                    IsPremium: false
                },
                disabled: {
                    Selected: false,
                    IsPremium: true
                }
            };

            this.limit = -1;
            this.selected = [];
            this.start_selected = [];
            this.TotalFilteredAttrs = [];
            this.category = '';
            this.accesslevel = '';

            this.data = {
                original: {},
                config: {},
                overview: {},
                buckets: {},
                lastRefresh: ''
            };

            this.saving = false;
        };

        this.set = function (property, value) {
            this[property] = value;
        };

        this.get = function (property) {
            return this[property];
        };

        this.setData = function (type, data) {
            this.data[type] = data;
        };

        this.getData = function (property) {
            return this.data[property];
        };

        this.getAccessRestriction = function () {
            var session = BrowserStorageUtility.getSessionDocument();

            if (session !== null || session.User !== null) {
                this.accesslevel = session.User.AccessLevel;
            }

            return this.accesslevel;
        };

        this.searchFilter = function (attr) {
            var text = store.get('filters').queryText.toLowerCase();

            if (text) {
                var SubCategory = (attr.SubCategory || '').toLowerCase();
                var DisplayName = attr.DisplayName.toLowerCase();
                var Description = (attr.Description || '').toLowerCase();

                var chkName = DisplayName.indexOf(text) >= 0;
                var chkSub = SubCategory.indexOf(text) >= 0;
                var chkDesc = Description.indexOf(text) >= 0;

                if (chkName || chkSub || chkDesc) {
                    return true;
                } else if (attr.Attributes) {
                    for (var i = 0; i < attr.Attributes.length; i++) {
                        var item = attr.Attributes[i];
                        var catName = item.DisplayName.toLowerCase();
                        var catDesc = (item.Description || '').toLowerCase();

                        if (catName.indexOf(text) >= 0 || catDesc.indexOf(text) >= 0) {
                            return true;
                        }
                    }
                }
            } else {
                return true;
            }

            return false;
        };

        this.getFiltering = function () {
            var filters = this.get('filters'),
                obj = {};

            Object.keys(filters.show).forEach(function (property) {
                if (filters.show[property] === true) {
                    obj[property] = true;
                }
            });

            Object.keys(filters.hide).forEach(function (property) {
                if (filters.hide[property] === true) {
                    obj[property] = false;
                }
            });

            return obj;
        };

        this.modalCallback = function (args) {
            var modal = Modal.get(args.name);
            var ret = true;

            switch (args.action) {
                case "closedForced":
                    break;

                case "cancel":
                    break;

                case "ok":
                    modal.waiting(true);
                    modal.disableDischargeButton(true);

                    store.saveConfig().then(function (result) {
                        var uiAction = result.data.uiAction;

                        if (args.name == 'AttrAdmin_Warning') {
                            Modal.modalRemoveFromDOM(modal, args);
                        }
                    });

                    ret = false;
                    break;
            }

            return ret;
        };

        this.getSection = function () {
            var map = {
                'home.attributes.activate': 'activate',
                'home.attributes.enable': 'enable',
                'home.attributes.edit': 'edit'
            };

            return map[$state.current.name];
        };

        this.getActiveTabData = function () {
            var page = this.getSection();
            var param = page == 'activate' ? 'category' : 'section';
            var active = $stateParams[param];
            var data = this.get('data').overview;
            var tab = [];

            if (data && data.Selections) {
                tab = data.Selections.filter(function (tab) {
                    return tab.DisplayName == active;
                });
            }

            return tab[0] || {};
        };

        this.getSelectedTotal = function () {
            var selected = this.selected;
            var section = this.getSection();
            var total = selected.length;
            var tab, started;

            if (section == 'enable') {
                started = this.get('start_selected');
                tab = this.getActiveTabData();

                if (tab.Selected) {
                    total = tab.Selected + (total - started.length);
                }
            }

            return total;
        };

        this.getUsageLimit = function (overview, area) {
            var section = this.getSection();
            var tab = overview.Selections.filter(function (tab) {
                return tab.DisplayName == area;
            })[0];

            return tab.Limit;
        };

        this.getBucketData = function (category, subcategory) {
            var deferred = $q.defer();

            if (['Intent', 'Technology Profile'].indexOf(category) < 0) {
                deferred.resolve([]);
            } else {
                AttrConfigService.getBucketData(category, subcategory).then(function (data) {
                    store.data.buckets[subcategory] = data.data;
                    deferred.resolve(data);
                });
            }

            return deferred.promise;
        };

        this.isChanged = function () {
            if (!this.data.original.Subcategories) {
                return false;
            }

            var hasChanged = false;
            var subcategories = this.data.original.Subcategories;

            this.data.config.Subcategories.forEach(function (subcategory, index) {
                subcategory.Attributes.forEach(function (attribute, i) {
                    if (subcategories[index].Attributes[i].Selected !== attribute.Selected) {
                        hasChanged = true;
                    }
                });
            });

            return hasChanged;
        };

        this.generatePayload = function () {
            var original = this.get('data').original;
            var activate = this.getSection() == 'activate';
            var data = {
                Select: [],
                Deselect: []
            };

            this.data.config.Subcategories.forEach(function (subcategory, index) {
                var oSub = original.Subcategories[index];

                subcategory.Attributes.forEach(function (attr, i) {
                    var oAttr = oSub.Attributes.filter(function (item) {
                        return attr.DisplayName == item.DisplayName;
                    });

                    if (oAttr.length === 0 || oAttr[0].Selected === attr.Selected) {
                        return;
                    }

                    if (attr.Selected) {
                        data.Select.push(attr.Attribute);
                    } else {
                        data.Deselect.push(attr.Attribute);
                    }
                });
            });

            return data;
        };

        this.saveConfig = function () {
            var deferred = $q.defer();
            var category = this.get('category');
            var activate = this.getSection() == 'activate';
            var type = activate ? 'activation' : 'usage';
            var data = this.generatePayload();
            var usage = {};

            store.set('saving', true);

            if (!activate) {
                usage.usage = $stateParams.section;
            }

            this.putConfig(type, category, usage, data).then(function (result) {
                $timeout(function () {
                    if (result.status >= 200 && result.status < 300) {
                        store.setData('original', JSON.parse(JSON.stringify(store.data.config)));

                        DataCloudStore.clear();

                        ShowSpinner('Refreshing Data');
                        $state.reload();
                    } else {
                        store.set('saving', false);
                    }

                    deferred.resolve(result);
                }, 500);
            });

            return deferred.promise;
        };

        this.putConfig = function (type, category, usage, data) {
            var deferred = $q.defer();

            AttrConfigService.putConfig(type, category, usage, data).then(function (data) {
                deferred.resolve(data);
            });

            return deferred.promise;
        };

        this.uiCanExit = function () {
            var isChanged = store.isChanged();

            if (!isChanged) {
                return true;
            }

            var deferred = $q.defer();

            Modal.warning({
                title: "Warning",
                message: "The changes you have made won't apply to the system until you save them.  Are you sure you want to leave the page without saving?",
                confirmtext: 'Yes, discard changes'
            }, function (opts) {
                switch (opts.action) {
                    case "ok":
                        deferred.resolve(true); break;
                    default:
                        deferred.reject("user cancelled action");
                        HideSpinner();
                }

                return true;
            });

            return deferred.promise;
        };

        this.refreshAttributes = () => { 
         var deferred = $q.defer();

			AttrConfigService.refreshAttributes(
            ).then(function (data) {
                data.lastRefresh ? store.data.lastRefresh : '';
				deferred.resolve(data);
			});

			return deferred.promise;   
        }

        this.init();
    })
    .service('AttrConfigService', function ($q, $http) {
        this.getOverview = function (section) {
            var deferred = $q.defer();

            $http({
                method: 'GET',
                url: '/pls/attrconfig/' + section + '/overview'
            }).then(function (response) {
                deferred.resolve(response);
            });

            return deferred.promise;
        };

        this.getConfig = function (section, category, params) {
            var deferred = $q.defer();

            $http({
                method: 'GET',
                url: '/pls/attrconfig/' + section + '/config/category/' + category,
                params: params
            }).then(function (response) {
                deferred.resolve(response);
            });

            return deferred.promise;
        };

        this.getBucketData = function (category, subcategory) {
            var deferred = $q.defer();

            $http({
                method: 'GET',
                url: '/pls/attrconfig/stats/category/' + category,
                params: {
                    'subcategory': subcategory
                }
            }).then(function (response) {
                deferred.resolve(response);
            });

            return deferred.promise;
        };

        this.putConfig = function (section, category, params, data) {
            var deferred = $q.defer();

            $http({
                method: 'PUT',
                url: '/pls/attrconfig/' + section + '/config/category/' + category,
                params: params,
                data: data
            }).then(function success(response) {
                deferred.resolve(response);
            }, function error(response) {
                deferred.resolve(response);
            });

            return deferred.promise;
        };
        this.refreshAttributes = function () { 
            var deferred = $q.defer();
            
            return deferred.promise;
        }
    });