angular.module('mainApp.appCommon.widgets.ManageFieldsWidget', [
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.setup.utilities.SetupUtility',
    'mainApp.setup.services.MetadataService',
    'mainApp.appCommon.services.ManageFieldsService',
    'mainApp.setup.modals.EditFieldModel',
    'mainApp.setup.controllers.DiscardEditFieldsModel',
    'kendo.directives'
])

.controller('ManageFieldsWidgetController', function ($scope, $rootScope, $timeout, StringUtility, ResourceUtility, SetupUtility, MetadataService, ManageFieldsService, EditFieldModel, DiscardEditFieldsModel) {
    $scope.TagsOptions = ["Internal", "External"];
    $scope.CategoryOptions = ["Lead Information", "Marketing Activity"];
    $scope.StatisticalTypeOptions = ["interval", "nominal", "ordinal", "ratio"];
    $scope.ApprovedUsageOptions = ["None", "Model", "ModelAndAllInsights", "ModelAndModelInsights"];
    $scope.FundamentalTypeOptions = ["boolean", "currency", "numeric", "percentage", "year"];

    $scope.ResourceUtility = ResourceUtility;
    $scope.saveInProgress = false;
    $scope.showFieldDetails = false;
    $scope.fieldAttributes = [];

    loadFields();

    function loadFields() {
        $scope.loading = true;
        MetadataService.GetFields().then(function(result) {
            if (result.Success) {
                $scope.fields = result.ResultObj;
                renderSelects($scope.fields);
                renderGrid($scope.fields);

                if ($scope.fields != null && $scope.fields.length > 0) {
                    $scope.fieldAttributes = [];
                    for (var attr in $scope.fields[0]) {
                        $scope.fieldAttributes.push(attr);
                    }
                }
            } else {
                $scope.showLoadingError = true;
                $scope.loadingError = result.ResultErrors;
                $scope.loading = false;
            }
        });
    }

    function renderSelects(fields) {
        var obj = ManageFieldsService.GetOptionsForSelects(fields);
        $scope.sourcesToSelect = obj.sourcesToSelect;
        $scope.categoriesToSelect = obj.categoriesToSelect;
        $scope.allOptions = obj.allOptions;
    }

    function renderGrid(fields) {
        $scope.dirtyRows = {};

        var grid = $("#fieldsGrid").data("kendoGrid");
        if (grid != null && grid.dataSource != null) {
            var state = kendo.stringify({
                page: grid.dataSource.page(),
                pageSize: grid.dataSource.pageSize(),
                sort: grid.dataSource.sort(),
                filter: grid.dataSource.filter()
            });

            grid.dataSource.data(fields);

            state = JSON.parse(state);
            if (state.page > 1 || state.sort != null || state.filter != null) {
                grid.dataSource.query(state);
            }

            $scope.loading = false;
        } else {
            var pageSize = fields.length;
            if (pageSize > 50) {
                pageSize = 50;
            }
            var dataSource = new kendo.data.DataSource({
                data: fields,
                schema: {
                    model: {
                        fields: {
                            ColumnName: { type: "string", editable: false },
                            SourceToDisplay: { type: "string", editable: false },
                            DisplayName: { type: "string" },
                            Tags: { type: "string" },
                            Category: { type: "string" },
                            ApprovedUsage: { type: "string" },
                            FundamentalType: { type: "string" }
                        }
                    }
                },
                pageSize: pageSize
            });

            $scope.gridOptions = {
                dataSource: dataSource,
                scrollable: false,
                sortable: { mode: "single", allowUnsort: false },
                pageable: {
                    messages: {
                        first: "",
                        previous: "",
                        next: "",
                        last: "",
                        display: "{0} - {1} of {2}",
                        empty: "No data"
                    }
                },
                columns: [
                    {
                        field: "ColumnName", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_FIELD'),
                        template: kendo.template($("#fieldTemplate").html()),
                        width: 184
                    },
                    {
                        field: "SourceToDisplay", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_SOURCE'),
                        template: kendo.template($("#sourceTemplate").html()),
                        width: 108
                    },
                    {
                        field: "DisplayName", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_DISPLAY_NAME'),
                        template: kendo.template($("#displayNameTemplate").html()),
                        width: 150
                    },
                    {
                        field: "Tags", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_TAGS'),
                        template: kendo.template($("#tagsTemplate").html()),
                        width: 90
                    },
                    {
                        field: "Category", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_CATEGORY'),
                        template: kendo.template($("#categoryTemplate").html()),
                        width: 120
                    },
                    {
                        field: "ApprovedUsage", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_APPROVED_USAGE'),
                        template: kendo.template($("#approvedUsageTemplate").html()),
                        width: 128
                    },
                    {
                        field: "FundamentalType", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_FUNDAMENTAL_TYPE'),
                        template: kendo.template($("#fundamentalTypeTemplate").html()),
                        width: 128
                    }
                ]
            };
            $scope.loading = false;
        }
    }

    $scope.categoryEditable = function(dataItem) {
        return ManageFieldsService.CategoryEditable(dataItem);
    };

    $scope.selectChanged = function($event, filerColumn) {
        if (filerColumn == "source") {
            sourceSelectChanged();
        }

        $scope.filterFields($event);
    };

    function sourceSelectChanged() {
        var objects = [];
        var categories = [];
        var allOptions = $scope.allOptions;
        var selectedSource = $scope.source;
        var sourceIsEmpty = StringUtility.IsEmptyString(selectedSource);
        for (var j = 0; j < allOptions.length; j++) {
            if (sourceIsEmpty || allOptions[j][0] == selectedSource) {
                var category = allOptions[j][1];
                if (!StringUtility.IsEmptyString(category) && categories.indexOf(category) < 0) {
                    categories.push(category);
                }
            }
        }
        $scope.categoriesToSelect = categories.sort();
        if ($scope.categoriesToSelect.indexOf($scope.category) < 0) {
            $scope.category = "";
        }
    }

    $scope.keyEnterFilter = function($event) {
        if ($event.keyCode === 13) {
            $scope.filterFields($event);
        }
    };

    $scope.filterFields = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        var orFilters = [];
        if (!StringUtility.IsEmptyString($scope.field)) {
            var nameFiler = {
                logic: "or",
                filters: [
                    { field: "ColumnName", operator: "contains", value: $scope.field },
                    { field: "DisplayName", operator: "contains", value: $scope.field }
                ]
            };
            orFilters.push(nameFiler);
        }

        var andFilters = [];
        if (!StringUtility.IsEmptyString($scope.source)) {
            andFilters.push({ field: "SourceToDisplay", operator: "eq", value: $scope.source });
        }
        if (!StringUtility.IsEmptyString($scope.category)) {
            andFilters.push({ field: "Category", operator: "eq", value: $scope.category });
        }

        var errorFilters = [];
        if ($scope.onlyShowErrorFields) {
            var columns = $scope.gridOptions.columns;
            for (var i = 0; i < columns.length; i++) {
                if (columns[i].field != "SourceToDisplay") {
                    errorFilters.push({ field: columns[i].field, operator: "eq", value: "" });
                }
            }
        }

        var filter;
        if (orFilters.length > 0 && andFilters.length > 0) {
            filter = {
                logic: "and",
                filters: [
                    { logic: "or", filters: orFilters },
                    { logic: "and", filters: andFilters }
                ]
            };
            if (errorFilters.length > 0) {
                filter.filters.push({ logic: "or", filters: errorFilters});
            }
        } else if (orFilters.length > 0) {
            if (errorFilters.length > 0) {
                filter = {
                    logic: "and",
                    filters: [
                        { logic: "or", filters: orFilters },
                        { logic: "or", filters: errorFilters }
                    ]
                };
            } else {
                filter = { logic: "or", filters: orFilters };
            }
        } else if (andFilters.length > 0) {
            if (errorFilters.length > 0) {
                filter = {
                    logic: "and",
                    filters: [
                        { logic: "and", filters: andFilters },
                        { logic: "or", filters: errorFilters }
                    ]
                };
            } else {
                filter = { logic: "and", filters: andFilters };
            }
        } else {
            if (errorFilters.length > 0) {
                filter = { logic: "or", filters: errorFilters };
            } else {
                filter = {};
            }
        }
        $("#fieldsGrid").data("kendoGrid").dataSource.filter(filter);
    };

    $scope.editClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        $scope.batchEdit = true;
    };

    $scope.saveClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        if ($scope.saveInProgress) { return; }
        $scope.saveInProgress = true;
        $scope.showEditFieldsError = false;

        var editedData = getEditedData();
        if (editedData != null && editedData.length > 0) {
            MetadataService.UpdateFields(editedData).then(function(result){
                if (result.Success) {
                    $scope.batchEdit = false;
                    $scope.saveInProgress = false;

                    loadFields();
                } else {
                    $scope.editFieldsErrorMessage = result.ResultErrors;
                    $scope.showEditFieldsError = true;
                    $scope.saveInProgress = false;
                }
            });
        } else {
            $scope.batchEdit = false;
            $scope.saveInProgress = false;
        }
    };

    $scope.cancelClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        if ($scope.saveInProgress) { return; }

        var editedData = getEditedData();
        if (editedData != null && editedData.length > 0) {
            DiscardEditFieldsModel.show($scope);
        } else {
            $scope.discardChanges();
        }
    };

    $scope.discardChanges = function () {
        $("#fieldsGrid").data("kendoGrid").cancelChanges();
        $scope.showEditFieldsError = false;
        $scope.batchEdit = false;
        $scope.dirtyRows = {};
    };

    $scope.textboxClicked = function ($event) {
        if ($scope.activeTextbox != $event.target) {
            $event.target.select();
            $scope.activeTextbox = $event.target;
        }
    };

    $scope.valueChanged = function ($event, dataItem, field, originalValue) {
        if ($event != null) {
            $event.preventDefault();
        }

        var dirtyRow = $scope.dirtyRows[dataItem.uid] || {};
        if (dirtyRow[field] == null) {
            dirtyRow[field] = { dirty: true, ov: originalValue };
        } else {
            var newValue = dataItem[field];
            dirtyRow[field].dirty = (newValue != dirtyRow[field].ov);
        }
        $scope.dirtyRows[dataItem.uid] = dirtyRow;

        dataItem.dirty = false;
        for (var f in dirtyRow) {
            if (dirtyRow[f].dirty) {
                dataItem.dirty = true;
                break;
            }
        }
    };

    function getEditedData() {
        var grid = $("#fieldsGrid").data("kendoGrid");
        return $.grep(grid.dataSource.data(), function(item) {
            return item.dirty;
        });
    }

    $scope.isChanged = function (uid, field) {
        var row = $scope.dirtyRows[uid];
        return row && row[field] && row[field].dirty;
    };

    $scope.fieldLinkClicked = function($event, field) {
        if ($event != null) {
            $event.preventDefault();
        }

        EditFieldModel.show(field, $scope);
    };

    $scope.$on(SetupUtility.LOAD_FIELDS_EVENT, function (event, data) {
        loadFields();
    });

    $scope.isEmpty = function(value) {
        return value == null || value === '';
    };
})

.directive('manageFieldsWidget', function () {
    return {
        templateUrl: 'app/AppCommon/widgets/manageFieldsWidget/ManageFieldsWidgetTemplate.html'
    };
});