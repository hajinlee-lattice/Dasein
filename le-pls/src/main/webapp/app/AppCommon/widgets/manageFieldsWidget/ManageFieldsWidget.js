angular.module('mainApp.appCommon.widgets.ManageFieldsWidget', [
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.setup.services.ManageFieldsService',
    'mainApp.setup.modals.EditFieldModel',
    'kendo.directives'
])

.controller('ManageFieldsWidgetController', function ($scope, $rootScope, StringUtility, ResourceUtility, ManageFieldsService, EditFieldModel) {
    $scope.ResourceUtility = ResourceUtility;

    $scope.loading = true;
    ManageFieldsService.GetFields().then(function(result) {
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

    function renderSelects(fields) {
        var allSources = [];
        var allObjects = [];
        var allCategories = [];
        var allOptions = [];
        for (var i = 0; i < fields.length; i++) {
            var field = fields[i];
            if (!StringUtility.IsEmptyString(field.Source) && allSources.indexOf(field.Source) < 0) {
                allSources.push(field.Source);
            }
            if (!StringUtility.IsEmptyString(field.Object) && allObjects.indexOf(field.Object) < 0) {
                allObjects.push(field.Object);
            }
            if (!StringUtility.IsEmptyString(field.Category) && allCategories.indexOf(field.Category) < 0) {
                allCategories.push(field.Category);
            }

            var exist = false; 
            for (var j = 0; j < allOptions.length; j++) {
                if (allOptions[j][0] == field.Source 
                    && allOptions[j][1] == field.Object 
                    && allOptions[j][2] == field.Category) {
                    exist = true;
                    break;
                }
            }
            if (!exist) {
                allOptions.push([field.Source, field.Object, field.Category]);
            }
        }
        $scope.sourcesToSelect = allSources.sort();
        $scope.objectsToSelect = allObjects.sort();
        $scope.categoriesToSelect = allCategories.sort();
        $scope.allOptions = allOptions;
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
                        Source: { type: "string", editable: false },
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
            sortable: true,
            scrollable: false,
            pageable: pageable,
            navigatable: true,
            columns: [
                {
                    field: "ColumnName", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_FIELD'),
                    template: kendo.template($("#fieldTemplate").html())
                },
                {
                    field: "Source", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_SOURCE'),
                    template: kendo.template($("#sourceTemplate").html())
                },
                {field: "Object", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_OBJECT') },
                {
                    field: "Category", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_CATEGORY'),
                    template: '#:Category == null ? "" : Category#<span data-ng-if="#:Category == null || Category == \'\'#" class=\'fa warning\'>(blank)&nbsp;</span>'
                },
                {
                    field: "DisplayName", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_DISPLAY_NAME'),
                    template: '#:DisplayName == null ? "" : DisplayName#<span data-ng-if="#:DisplayName == null || DisplayName == \'\'#" class=\'fa warning\'>(blank)&nbsp;</span>'
                },
                {
                    field: "ApprovedUsage", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_APPROVED_USAGE'),
                    //editor: approvedUsageEditor,
                    template: '#:ApprovedUsage == null ? "" : ApprovedUsage#<span data-ng-if="#:ApprovedUsage == null || ApprovedUsage == \'\'#" class=\'fa warning\'>(blank)&nbsp;</span>'
                },
                {
                    field: "Tags", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_TAGS'),
                    template: '#:Tags == null ? "" : Tags#<span data-ng-if="#:Tags == null || Tags == \'\'#" class=\'fa warning\'>(blank)&nbsp;</span>'
                },
                {
                    field: "FundamentalType", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_FUNDAMENTAL_TYPE'),
                    template: '#:FundamentalType == null ? "" : FundamentalType#<span data-ng-if="#:FundamentalType == null || FundamentalType == \'\'#" class=\'fa warning\'>(blank)&nbsp;</span>'
                }
            ]
        };
    }

    function approvedUsageEditor(container, options) {
        $('<input required data-bind="value:' + options.field + '"/>')
        .appendTo(container)
        .kendoDropDownList({
            autoBind: false,
            dataSource: $scope.widgetConfig["ApprovedUsageOptions"]
        });
    }

    $scope.selectChanged = function($event, filerColumn) {
        var allOptions = $scope.allOptions;
        if (filerColumn == "source") {
            var objects = [];
            var categories = [];
            var selectedSource = $scope.source;
            var sourceIsEmpty = StringUtility.IsEmptyString(selectedSource);
            for (var i = 0; i < allOptions.length; i++) {
                if (sourceIsEmpty || allOptions[i][0] == selectedSource) {
                    var object = allOptions[i][1];
                    if (!StringUtility.IsEmptyString(object) && objects.indexOf(object) < 0) {
                        objects.push(object);
                    }
                }
            }
            for (var i = 0; i < allOptions.length; i++) {
                if (sourceIsEmpty || allOptions[i][0] == selectedSource) {
                    var category = allOptions[i][2];
                    if (!StringUtility.IsEmptyString(category) && categories.indexOf(category) < 0) {
                        categories.push(category);
                    }
                }
            }
            $scope.objectsToSelect = objects.sort();
            $scope.categoriesToSelect = categories.sort();
        } else if (filerColumn == "object") {
            var categories = [];
            var selectedSource = $scope.source;
            var selectedObject = $scope.object;
            var sourceIsEmpty = StringUtility.IsEmptyString(selectedSource);
            var objectIsEmpty = StringUtility.IsEmptyString(selectedObject);
            for (var i = 0; i < allOptions.length; i++) {
                if ((sourceIsEmpty || allOptions[i][0] == selectedSource) 
                    && (objectIsEmpty || allOptions[i][1] == selectedObject)) {
                    var category = allOptions[i][2];
                    if (!StringUtility.IsEmptyString(category) && categories.indexOf(category) < 0) {
                        categories.push(category);
                    }
                }
            }
            $scope.categoriesToSelect = categories.sort();
        } else if (filerColumn == "category") {
            var objects = [];
            var selectedSource = $scope.source;
            var selectedCategory = $scope.category;
            var sourceIsEmpty = StringUtility.IsEmptyString(selectedSource);
            var categoryIsEmpty = StringUtility.IsEmptyString(selectedCategory);
            for (var i = 0; i < allOptions.length; i++) {
                if ((sourceIsEmpty || allOptions[i][0] == selectedSource) 
                    && (categoryIsEmpty || allOptions[i][2] == selectedCategory)) {
                    var object = allOptions[i][1];
                    if (!StringUtility.IsEmptyString(object) && objects.indexOf(object) < 0) {
                        objects.push(object);
                    }
                }
            }
            $scope.objectsToSelect = objects.sort();
        }

        $scope.filterFields($event);
    }

    $scope.keyEnterFilter = function($event) {
        if ($event.keyCode === 13) {
            $scope.filterFields($event);
        }
    }

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
            andFilters.push({ field: "Source", operator: "eq", value: $scope.source });
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
                if (columns[i].field != "Source" && columns[i].field != "Object") {
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
                }
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
                }
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
    }

    $scope.editButtonClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        $scope.editable = true;
        if ($scope.editable) {
            $("#fieldsGrid").data("kendoGrid").setOptions({editable: true});
        } else {
            $("#fieldsGrid").data("kendoGrid").setOptions({editable: false});
        }
    }

    $scope.saveButtonClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        var data = $("#fieldsGrid").data("kendoGrid");
        data.saveChanges();
        data.setOptions({editable: false});
        $scope.editable = false;
    }

    $scope.cancelButtonClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        var data = $("#fieldsGrid").data("kendoGrid");
        data.cancelChanges();
        data.setOptions({editable: false});
        $scope.editable = false;
    }

    $scope.fieldLinkClicked = function($event, field) {
        if ($event != null) {
            $event.preventDefault();
        }

        EditFieldModel.show(field);
    }
})

.directive('manageFieldsWidget', function () {
    return {
        templateUrl: 'app/AppCommon/widgets/manageFieldsWidget/ManageFieldsWidgetTemplate.html'
    };
});