var app = angular.module("app.tenants.controller.TenantListCtrl", [
    'le.common.util.UnderscoreUtility',
    'app.tenants.service.TenantService',
    'kendo.directives',
    'ui.bootstrap'
]);

app.controller('TenantListCtrl', function($scope, $state, _, $modal, TenantService, TenantUtility) {
    $scope.loading = true;

    TenantService.GetAllTenants().then(function(result){
        if (result.success) {
            $scope.tenants = result.resultObj;
            $scope.renderGrid($scope.tenants);
        } else {
            //TODO:song handle error
        }
        $scope.loading = false;
    });

    $scope.renderGrid = function(tenants) {
        var pageable = false;
        var pageSize = tenants.length;

        // paging if too many result
        if (tenants.length > 10) {
            pageSize = 10;
            pageable = true;
        }

        var dataSource = new kendo.data.DataSource({
            data: tenants,
            schema: {
                model: {
                    fields: {
                        TenantId: { type: "string" },
                        DisplayName: { type: "string" },
                        VDB: { type: "string" },
                        PLS: { type: "string" },
                        Dante: { type: "string" },
                        GlobalAuth: { type: "string" },
                        DataLoader: {type: "string"},
                        TPL: {type: "string"}
                    }
                }
            },
            pageSize: pageSize
        });

        $scope.gridOptions = {
            dataSource: dataSource,
            sortable: true,
            pageable: pageable,
            selectable: "row",
            filterable: {
                extra: false
            },
            columns: [
                {field: "TenantId", title: "Tenant ID"},
                {field: "DisplayName", title: "Name"},
                {
                    field: "VDB",
                    template: function(dataItem){
                        return TenantUtility.getStatusTemplate(dataItem.VDB);
                    },
                    filterable: {
                        operators: {
                            string: {
                                eq: "Is equal to",
                                neq: "Is not equal to"
                            }
                        },
                        ui: statusFilter
                    }
                },
                {
                    field: "DataLoader",
                    title:"DL",
                    template: function(dataItem){
                        return TenantUtility.getStatusTemplate(dataItem.DataLoader);
                    },
                    filterable: {
                        operators: {
                            string: {
                                eq: "Is equal to",
                                neq: "Is not equal to"
                            }
                        },
                        ui: statusFilter
                    }
                },
                {
                    field: "PLS",
                    template: function(dataItem){
                        return TenantUtility.getStatusTemplate(dataItem.PLS);
                    },
                    filterable: {
                        operators: {
                            string: {
                                eq: "Is equal to",
                                neq: "Is not equal to"
                            }
                        },
                        ui: statusFilter
                    }
                },
                {
                    field: "Dante",
                    template: function(dataItem){
                        return TenantUtility.getStatusTemplate(dataItem.Dante);
                    },
                    filterable: {
                        operators: {
                            string: {
                                eq: "Is equal to",
                                neq: "Is not equal to"
                            }
                        },
                        ui: statusFilter
                    }
                },
                {
                    field: "GlobalAuth",
                    title: "GA",
                    template: function(dataItem){
                        return TenantUtility.getStatusTemplate(dataItem.GlobalAuth);
                    },
                    filterable: {
                        operators: {
                            string: {
                                eq: "Is equal to",
                                neq: "Is not equal to"
                            }
                        },
                        ui: statusFilter
                    }
                },
                {
                    field: "TPL", template: function(dataItem){
                        return TenantUtility.getStatusTemplate(dataItem.TPL);
                    },
                    filterable: {
                        operators: {
                            string: {
                                eq: "Is equal to",
                                neq: "Is not equal to"
                            }
                        },
                        ui: statusFilter
                    }
                }
            ]
        };
    };

    function statusFilter(element) {
        element.kendoDropDownList({
            dataSource: ["OK", "FAILED", "INITIAL", "INSTALLING"],
            optionLabel: "--Select Value--"
        });
    }

    $scope.handleKendoChange = function(data) {
        $state.go('TENANT.CONFIG', {tenantId: data.TenantId});
    };

    $scope.onAddClick = function(){
        $scope.cleanData = TenantUtility.cleanupComponentConfigs($scope.data);

        var modalInstance = $modal.open({
            templateUrl: 'addNewTenantModal.html',
            resolve: {
                TenantUtility: function () {
                    return TenantUtility;
                }
            },
            controller: function($scope, $modalInstance, TenantUtility){
                $scope.spaceOptions = ["production", "sandbox"];

                $scope.tenantInfo = {
                    space: "production"
                };

                $scope.isValid = true;

                $scope.validateTenantId = function(){
                    if ($scope.addtenantform.tenantId.$dirty && $scope.addtenantform.tenantId.$error.required) {
                        $scope.tenantIdErrorMsg = "Tenant ID is required.";
                        $scope.showTenantIdError = true;
                        $scope.isValid = false;
                        return;
                    }

                    var validationResult = TenantUtility.validateTenantId($scope.tenantInfo.tenantId);
                    if (!validationResult.valid) {
                        $scope.tenantIdErrorMsg = validationResult.reason;
                        $scope.showTenantIdError = true;
                        $scope.isValid = false;
                    } else {
                        $scope.showTenantIdError = false;
                        $scope.isValid = true;
                    }
                };

                $scope.ok = function () {
                    $modalInstance.close($scope.tenantInfo);
                };

                $scope.cancel = function () {
                    $modalInstance.dismiss('cancel');
                };
            }
        });

        modalInstance.result.then(function (tenantInfo) {
            $scope.tenantInfo = tenantInfo;
            $state.go('TENANT.CONFIG', {tenantId: tenantInfo.tenantId, mode: "NEW"});
        }, function () {
            $state.go('TENANT.LIST');
        });

    };

});
