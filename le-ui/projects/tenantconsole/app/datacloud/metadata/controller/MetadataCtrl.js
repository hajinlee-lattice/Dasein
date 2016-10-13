angular.module("app.datacloud.controller.MetadataCtrl", [
    'kendo.directives',
    'ngAnimate',
    'ngSanitize',
    'ui.bootstrap',
    'jsonFormatter'
])
.controller('AttributeDetailModalCtrl', function($uibModalInstance, data){
    this.data = JSON.parse(JSON.stringify(data));
    this.close = function () {
        $uibModalInstance.dismiss();
    };
})
.controller('MetadataCtrl', function ($scope, $state, $timeout, $uibModal, MetadataService) {

    $scope.loadData = function(version) {
        $scope.loadingData = true;

        MetadataService.GetAccountMasterColumns(version).then(function(result){
            if (result.success) {
                $scope.numAttributes = result.resultObj.length;
                $scope.renderGrid(result.resultObj);
            } else {
                $scope.loadingError = result.errMsg;
                $scope.showLoadingError = false;
            }
            $scope.loadingData = false;
        });
    };

    //TODO: for not always load 2.0.0, in M6 we should change to a drop down of available versions.
    $scope.loadData('2.0.0');

    $scope.renderGrid = function(columns) {
        var pageable = true;
        var pageSize = 20;

        var dataSource = new kendo.data.DataSource({
            data: columns,
            schema: {
                model: {
                    fields: {
                        ColumnID: { type: "string" },
                        DisplayName: { type: "string" },
                        Category: { type: "string" },
                        Subcategory: { type: "string" },
                        FundamentalType: { type: "string" },
                        StatisticalType: { type: "string" },
                        ApprovedUsage: { type: "string" },
                        CanEnrich: {type: "boolean"},
                        IsPremium: {type: "boolean"}
                    }
                }
            },
            pageSize: pageSize
        });

        $scope.gridOptions = {
            dataSource: dataSource,
            sortable: true,
            scrollable: false,
            resizable: true,
            pageable: pageable,
            selectable: true,
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
                {field: "ColumnId", title: "Column Name"},
                {field: "DisplayName", title: "Display Name"},
                "Category",
                "Subcategory",
                "ApprovedUsage",
                {field: "FundamentalType", title: "FundType"},
                {field: "StatisticalType", title: "StatType"},
                {field: "CanEnrich", title: "Can Enrich", filterable: true},
                {field: "IsPremium", title: "Is Premium", filterable: true}
            ]
        };
    };

    $scope.openAttributeDetail = function(data) {
        console.log(data);
        $scope.dataForDetail = data;
        var modalInstance = $uibModal.open({
            animation: true,
            ariaLabelledBy: 'modal-title',
            ariaDescribedBy: 'modal-body',
            templateUrl: 'attributeDetailModal.html',
            controller: 'AttributeDetailModalCtrl',
            controllerAs: '$ctrl',
            resolve: {
                data: function () {
                    return $scope.dataForDetail;
                }
            }
        });

        modalInstance.result.then(function () {
            $log.info('Modal dismissed at: ' + new Date());
        });
    };


});
