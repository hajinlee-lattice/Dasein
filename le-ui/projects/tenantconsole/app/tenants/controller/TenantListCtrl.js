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
            $scope.showLoadingError = true;
            $scope.loadingError = result.errMsg;
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
                        ContractId: { type: "string" },
                        DisplayName: { type: "string" },
                        CreatedDate: { type: "date" },
                        LastModifiedDate: { type: "date" },
                        Product: { type: "string" },
                        Status: { type: "string" }
                    }
                }
            },
            pageSize: pageSize
        });

        $scope.gridOptions = {
            dataSource: dataSource,
            sortable: true,
            scrollable: false,
            pageable: pageable,
            selectable: "row",
            filterable: {
                operators: {
                    string: {
                        contains: "Contains",
                        doesnotcontain: "Does not contain",
                        eq: "Is equal to",
                        neq: "Is not equal to"
                    }
                },
                extra: false
            },
            columns: [
                {field: "TenantId", title: "Tenant ID"},
                {field: "ContractId", title: "Contract ID"},
                {field: "DisplayName", title: "Tenant Name"},
                {field: "CreatedDate", title: "Created Date", format: "{0:yyyy-MM-dd HH:mm:ss }"},
                {field: "LastModifiedDate", title: "Last Modified Date", format: "{0:yyyy-MM-dd HH:mm:ss }", width: 150},
                "Product",
                {
                    field: "Status",
                    template: function(dataItem){
                        return TenantUtility.getStatusTemplate(dataItem.Status);
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
            dataSource: _.map(["OK", "ERROR", "INITIAL", "UNKNOWN"], TenantUtility.getStatusDisplayName),
            optionLabel: "--Select Value--"
        });
    }

    $scope.handleKendoChange = function(data) {
        $state.go('TENANT.CONFIG', {tenantId: data.TenantId, contractId: data.ContractId});
    };

    $scope.onAddClick = function(){
        var modalInstance = $modal.open({
            templateUrl: 'addNewTenantModal.html',
            resolve: {
                TenantUtility: function () { return TenantUtility; },
                tenants: function () { return $scope.tenants; }
            },
            controller: function($scope, $modalInstance, _, tenants, TenantUtility){
                $scope.tenants = tenants;

                $scope.tenantInfo = {};
                $scope.isValid = true;

                $scope.validateTenantInfo = function(){
                    if ($scope.addtenantform.tenantId.$error.required || $scope.tenantInfo.tenantId === '') {
                        $scope.tenantIdErrorMsg = "Tenant ID is required.";
                        $scope.showTenantIdError = true;
                        $scope.isValid = false;
                        return false;
                    }

                    if (_.findWhere($scope.tenants, {TenantId: $scope.tenantInfo.tenantId})) {
                        $scope.tenantIdErrorMsg = "Tenant ID already exists.";
                        $scope.showTenantIdError = true;
                        $scope.isValid = false;
                        return false;
                    }

                    var validationResult = TenantUtility.validateTenantId($scope.tenantInfo.tenantId);
                    if (!validationResult.valid) {
                        $scope.tenantIdErrorMsg = validationResult.reason;
                        $scope.showTenantIdError = true;
                        $scope.isValid = false;
                        return false;
                    } else {
                        $scope.showTenantIdError = false;
                        $scope.isValid = true;
                    }

                    return true;
                };

                $scope.ok = function () {
                    if ($scope.validateTenantInfo()) {
                        if (!$scope.tenantInfo.hasOwnProperty("contractId") || $scope.tenantInfo.contractId === '') {
                            $scope.tenantInfo.contractId = $scope.tenantInfo.tenantId;
                        }
                        $modalInstance.close($scope.tenantInfo);
                    }
                };

                $scope.cancel = function () {
                    $modalInstance.dismiss('cancel');
                };
            }
        });

        modalInstance.result.then(function (tenantInfo) {
            $scope.tenantInfo = tenantInfo;
            $state.go('TENANT.CONFIG', {
                contractId: tenantInfo.contractId,
                tenantId: tenantInfo.tenantId,
                new: true,
                readonly: false,
                listenState: false
            });
        }, function () {
            $state.go('TENANT.LIST');
        });

    };

});
