var app = angular.module("app.tenants.controller.TenantListCtrl", [
    'le.common.util.UnderscoreUtility',
    'app.tenants.service.TenantService',
    'kendo.directives'
]);

app.controller('TenantListCtrl', function($scope, $state, _, TenantService, TenantUtility) {
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
            filterable: true,
            pageable: pageable,
            selectable: "row",
            columns: [
                {field: "TenantId", title: "Tenant ID"},
                {field: "DisplayName", title: "Name"},
                {field: "VDB", template: function(dataItem){
                    return TenantUtility.getStatusTemplate(dataItem.VDB);
                }},
                {field: "DataLoader", title:"DL", template: function(dataItem){
                    return TenantUtility.getStatusTemplate(dataItem.DataLoader);
                }},
                {field: "PLS", template: function(dataItem){
                    return TenantUtility.getStatusTemplate(dataItem.PLS);
                }},
                {field: "Dante", template: function(dataItem){
                    return TenantUtility.getStatusTemplate(dataItem.Dante);
                }},
                {field: "GlobalAuth", title: "GA", template: function(dataItem){
                    return TenantUtility.getStatusTemplate(dataItem.GlobalAuth);
                }},
                {field: "TPL", template: function(dataItem){
                    return TenantUtility.getStatusTemplate(dataItem.TPL);
                }}
            ]
        };
    };

    $scope.handleKendoChange = function(data) {
        $state.go('TENANT.CONFIG', {tenantId: data.TenantId});
    };

});
