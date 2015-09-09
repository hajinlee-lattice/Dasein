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

.controller('ManageFieldsWidgetController', function ($scope, $rootScope, StringUtility, ResourceUtility, SetupUtility, MetadataService, ManageFieldsService, EditFieldModel, DiscardEditFieldsModel) {
    $scope.TagsOptions = ["Internal", "External"];
    $scope.CategoryOptions = ["Lead Information", "Marketing Activity"];
    $scope.StatisticalTypeOptions = ["nominal", "ordinal", "interval", "ratio"];
    $scope.ApprovedUsageOptions = ["None", "Model", "ModelAndModelInsights", "ModelAndAllInsights"];
    $scope.FundamentalTypeOptions = ["currency", "email", "probability", "percent", "phone", "enum", "URI"];

    $scope.ResourceUtility = ResourceUtility;
    $scope.saveInProgress = false;
    $scope.showFieldDetails = false;

    loadFields();

    function loadFields() {
        $scope.loading = true;
        MetadataService.GetFields().then(function(result) {
            if (result.Success) {
                $scope.fields = result.ResultObj;
                renderSelects($scope.fields);
                renderGrid($scope.fields);
            } else {
                $scope.showLoadingError = true;
                $scope.loadingError = result.ResultErrors;
            }
            $scope.loading = false;
        });
    }

    function renderSelects(fields) {
        var obj = ManageFieldsService.GetOptionsForSelects(fields);
        $scope.sourcesToSelect = obj.sourcesToSelect;
        //$scope.objectsToSelect = obj.objectsToSelect;
        $scope.categoriesToSelect = obj.categoriesToSelect;
        $scope.allOptions = obj.allOptions;
    }

    function renderGrid(fields) {
        var pageable = false;
        var pageSize = fields.length;

        if (fields.length > 10) {
            pageSize = 10;
            pageable = true;
        }

        var dataSource = new kendo.data.DataSource({
            data: fields,
            schema: {
                model: {
                    fields: {
                        ColumnName: { type: "string", editable: false },
                        SourceToDisplay: { type: "string", editable: false },
                        Object: { type: "string", editable: false },
                        Category: { type: "string" },
                        DisplayName: { type: "string" },
                        ApprovedUsage: { type: "string" },
                        Tags: { type: "string" },
                        FundamentalType: { type: "string" }
                    }
                }
            },
            pageSize: pageSize
        });

        $scope.gridOptions = {
            dataSource: dataSource,
            pageable: pageable,
            sortable: true,
            scrollable: false,
            navigatable: true,
            edit: function(e) {
                if (e.container.find("input").attr("data-bind") === "value:Category" &&
                    !$scope.categoryEditable(e.model)) {
                    this.closeCell();
                }
            },
            columns: [
                {
                    field: "ColumnName", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_FIELD'),
                    template: kendo.template($("#fieldTemplate").html()),
                    width: 200
                },
                {
                    field: "SourceToDisplay", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_SOURCE'),
                    template: kendo.template($("#sourceTemplate").html()),
                    width: 110
                },
                /*{
                    field: "Object", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_OBJECT'),
                    template: kendo.template($("#objectTemplate").html()),
                    width:60
                },*/
                {
                    field: "Category", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_CATEGORY'),
                    editor: categoryEditor,
                    template: kendo.template($("#categoryTemplate").html()),
                    width: 120
                },
                {
                    field: "DisplayName", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_DISPLAY_NAME'),
                    template: kendo.template($("#displayNameTemplate").html()),
                    width: 160
                },
                {
                    field: "ApprovedUsage", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_APPROVED_USAGE'),
                    editor: approvedUsageEditor,
                    template: kendo.template($("#approvedUsageTemplate").html()),
                    width: 120
                },
                {
                    field: "Tags", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_TAGS'),
                    editor: tagsEditor,
                    template: kendo.template($("#tagsTemplate").html()),
                    width: 80
                },
                {
                    field: "FundamentalType", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_FUNDAMENTAL_TYPE'),
                    editor: fundamentalTypeEditor,
                    template: kendo.template($("#fundamentalTypeTemplate").html()),
                    width: 120
                }
            ]
        };
    }

    function approvedUsageEditor(container, options) {
        $('<input data-bind="value:' + options.field + '"/>')
        .appendTo(container)
        .kendoDropDownList({
            autoBind: false,
            dataSource: $scope.ApprovedUsageOptions
        });
    }

    function tagsEditor(container, options) {
        $('<input data-bind="value:' + options.field + '"/>')
        .appendTo(container)
        .kendoDropDownList({
            autoBind: false,
            dataSource: $scope.TagsOptions
        });
    }

    function categoryEditor(container, options) {
        $('<input data-bind="value:' + options.field + '"/>')
        .appendTo(container)
        .kendoDropDownList({
            autoBind: false,
            dataSource: $scope.CategoryOptions
        });
    }

    $scope.categoryEditable = function(dataItem) {
        return ManageFieldsService.CategoryEditable(dataItem);
    };

    function fundamentalTypeEditor(container, options) {
        $('<input data-bind="value:' + options.field + '"/>')
        .appendTo(container)
        .kendoDropDownList({
            autoBind: false,
            dataSource: $scope.FundamentalTypeOptions
        });
    }

    $scope.selectChanged = function($event, filerColumn) {
        if (filerColumn == "source") {
            sourceSelectChanged();
        }
        /*} else if (filerColumn == "object") {
            objectSelectChanged();
        } else if (filerColumn == "category") {
            categorySelectChanged();
        }*/

        $scope.filterFields($event);
    };

    function sourceSelectChanged() {
        var objects = [];
        var categories = [];
        var allOptions = $scope.allOptions;
        var selectedSource = $scope.source;
        var sourceIsEmpty = StringUtility.IsEmptyString(selectedSource);
        /*for (var i = 0; i < allOptions.length; i++) {
            if (sourceIsEmpty || allOptions[i][0] == selectedSource) {
                var object = allOptions[i][1];
                if (!StringUtility.IsEmptyString(object) && objects.indexOf(object) < 0) {
                    objects.push(object);
                }
            }
        }*/
        for (var j = 0; j < allOptions.length; j++) {
            if (sourceIsEmpty || allOptions[j][0] == selectedSource) {
                var category = allOptions[j][2];
                if (!StringUtility.IsEmptyString(category) && categories.indexOf(category) < 0) {
                    categories.push(category);
                }
            }
        }
        //$scope.objectsToSelect = objects.sort();
        $scope.categoriesToSelect = categories.sort();
    }

    /*function objectSelectChanged() {
        var categories = [];
        var allOptions = $scope.allOptions;
        var selectedSource = $scope.source;
        var selectedObject = $scope.object;
        var sourceIsEmpty = StringUtility.IsEmptyString(selectedSource);
        var objectIsEmpty = StringUtility.IsEmptyString(selectedObject);
        for (var i = 0; i < allOptions.length; i++) {
            if ((sourceIsEmpty || allOptions[i][0] == selectedSource) &&
                    (objectIsEmpty || allOptions[i][1] == selectedObject)) {
                var category = allOptions[i][2];
                if (!StringUtility.IsEmptyString(category) && categories.indexOf(category) < 0) {
                    categories.push(category);
                }
            }
        }
        $scope.categoriesToSelect = categories.sort();
    }*/

    /*function categorySelectChanged() {
        var objects = [];
        var allOptions = $scope.allOptions;
        var selectedSource = $scope.source;
        var selectedCategory = $scope.category;
        var sourceIsEmpty = StringUtility.IsEmptyString(selectedSource);
        var categoryIsEmpty = StringUtility.IsEmptyString(selectedCategory);
        for (var i = 0; i < allOptions.length; i++) {
            if ((sourceIsEmpty || allOptions[i][0] == selectedSource) &&
                    (categoryIsEmpty || allOptions[i][2] == selectedCategory)) {
                var object = allOptions[i][1];
                if (!StringUtility.IsEmptyString(object) && objects.indexOf(object) < 0) {
                    objects.push(object);
                }
            }
        }
        $scope.objectsToSelect = objects.sort();
    }*/

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
                    { field: "Field", operator: "contains", value: $scope.field },
                    { field: "DisplayName", operator: "contains", value: $scope.field }
                ]
            };
            orFilters.push(nameFiler);
        }

        var andFilters = [];
        if (!StringUtility.IsEmptyString($scope.source)) {
            andFilters.push({ field: "SourceToDisplay", operator: "eq", value: $scope.source });
        }
        if (!StringUtility.IsEmptyString($scope.object)) {
            andFilters.push({ field: "Object", operator: "eq", value: $scope.object });
        }
        if (!StringUtility.IsEmptyString($scope.category)) {
            andFilters.push({ field: "Category", operator: "eq", value: $scope.category });
        }

        var errorFilters = [];
        if ($scope.onlyShowErrorFields) {
            var columns = $scope.gridOptions.columns;
            for (var i = 0; i < columns.length; i++) {
                if (columns[i].field != "SourceToDisplay" && columns[i].field != "Object") {
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
        if ($scope.batchEdit) {
            $("#fieldsGrid").data("kendoGrid").setOptions({editable: true});
        } else {
            $("#fieldsGrid").data("kendoGrid").setOptions({editable: false});
        }
    };

    $scope.saveClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        if ($scope.saveInProgress) { return; }
        $scope.saveInProgress = true;
        $scope.showEditFieldsError = false;

        var grid = $("#fieldsGrid").data("kendoGrid");
        var editedData = $.grep(grid.dataSource.data(), function(item) {
            return item.dirty;
        });
        if (editedData != null && editedData.length > 0) {
            MetadataService.UpdateFields(editedData).then(function(result){
                if (result.Success) {
                    grid.setOptions({editable: false});
                    $scope.batchEdit = false;
                    $scope.saveInProgress = false;

                    reloadFields();
                } else {
                    $scope.editFieldsErrorMessage = result.ResultErrors;
                    $scope.showEditFieldsError = true;
                    $scope.saveInProgress = false;
                }
            });
        } else {
            grid.setOptions({editable: false});
            $scope.batchEdit = false;
            $scope.saveInProgress = false;
        }
    };

    $scope.cancelClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        if ($scope.saveInProgress) { return; }

        DiscardEditFieldsModel.show($scope);
    };

    $scope.fieldLinkClicked = function($event, field) {
        if ($event != null) {
            $event.preventDefault();
        }

        EditFieldModel.show(field, $scope);
    };

    $scope.$on(SetupUtility.LOAD_FIELDS_EVENT, function (event, data) {
        reloadFields();
    });

    function reloadFields() {
        loadFields();
        $("#fieldsGrid").data("kendoGrid").dataSource.read();
    }
})

.directive('manageFieldsWidget', function () {
    return {
        templateUrl: 'app/AppCommon/widgets/manageFieldsWidget/ManageFieldsWidgetTemplate.html'
    };
});