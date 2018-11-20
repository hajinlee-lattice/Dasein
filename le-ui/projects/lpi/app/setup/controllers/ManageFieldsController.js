angular.module('lp.managefields', [
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.setup.utilities.SetupUtility',
    'mainApp.setup.services.MetadataService',
    'mainApp.setup.controllers.DiscardEditFieldsModel',
    'mainApp.setup.modals.UpdateFieldsModal',
    'mainApp.setup.modals.FieldMappingSettingsModal'
])
.controller('ManageFieldsController', function (
    $scope, $rootScope, $timeout, $state, StringUtility, ResourceUtility, SetupUtility, NavUtility, MetadataService,
    DiscardEditFieldsModel, UpdateFieldsModal, FieldMappingSettingsModal, Model, HealthService) {

    $scope.ResourceUtility = ResourceUtility;
    $scope.saveInProgress = false;
    $scope.showFieldDetails = false;
    $scope.fieldAttributes = [];
    $scope.eventTableName = Model.EventTableProvenance.EventTableName;
    $scope.modelId = Model.ModelId;
    $scope.dirtyRows = {};
    $scope.indexToOldFieldsForSingleFieldPage = {};
    $scope.indexToOldFieldsForListFieldsPage = {};
    $scope.oneLeadPerDomain = Model.EventTableProvenance.Is_One_Lead_Per_Domain == null ? false :
        Model.EventTableProvenance.Is_One_Lead_Per_Domain == "true";
    $scope.includePersonalEmailDomains = Model.EventTableProvenance.Exclude_Public_Domains == null ? true :
        Model.EventTableProvenance.Exclude_Public_Domains == "false";
    $scope.useLatticeAttributes = Model.EventTableProvenance.Exclude_Propdata_Columns == null ? true :
        Model.EventTableProvenance.Exclude_Propdata_Columns == "false";
    $scope.enableTransformations = (Model.EventTableProvenance.Transformation_Group_Name == "none" ||
        Model.ModelDetails.TransformationGroupName == "none") ? false : true;
    $scope.sourceType = Model.ModelDetails.SourceSchemaInterpretation;

    getOptionsAndFields();

    function getOptionsAndFields() {
        $scope.loading = true;
        MetadataService.GetOptions().then(function(result) {
            if (result.Success) {
                var options = result.ResultObj;
                $scope.CategoryOptions = options.CategoryOptions;
                $scope.ApprovedUsageOptions = options.ApprovedUsageOptions;
                $scope.StatisticalTypeOptions = options.StatisticalTypeOptions;
                $scope.FundamentalTypeOptions = options.FundamentalTypeOptions;

                loadFields();
            } else {
                $scope.showLoadingError = true;
                $scope.loadingError = result.ResultErrors;
                $scope.loading = false;
            }
        });
    }

    function getFieldsToDisplay(metadataFields) {
        var fields = [];
        $scope.fieldsNotDisplayed = [];
        for (var i = 0; i < metadataFields.length; i++) {
            var dataItem = metadataFields[i];
            if (dataItem == null) {
                continue;
            }

            if (dataItem.ApprovedUsage == null || dataItem.ApprovedUsage.toLowerCase() == "none") {
                dataItem.ApprovedUsage = "None";
            }

            if (dataItem.ApprovedUsage != "None" || ! MetadataService.IsLatticeAttribute(dataItem)) {
                fields.push(dataItem);
            } else {
                $scope.fieldsNotDisplayed.push(dataItem);
            }
        }
        return fields;
    }

    function loadFields() {
        $scope.loading = true;
        MetadataService.GetMetadataForModel($scope.modelId).then(function(result) {
            $scope.fields = getFieldsToDisplay(result.ResultObj);
            renderSelects($scope.fields);
            renderGrid($scope.fields);

            if ($scope.fields != null && $scope.fields.length > 0) {
                $scope.fieldAttributes = [];
                for (var attr in $scope.fields[0]) {
                    $scope.fieldAttributes.push(attr);
                }
            }
            $scope.loading = false;
        });
    }

    function renderSelects(fields) {
        var obj = MetadataService.GetOptionsForSelects(fields);
        $scope.categoriesToSelect = obj.categoriesToSelect;
        $scope.allOptions = obj.allOptions;
    }

    function renderGrid(fields) {
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
                            DisplayName: { type: "string" },
                            ColumnName: { type: "string" },
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
                        field: "DisplayName", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_DISPLAY_NAME'),
                        template: kendo.template($("#displayNameTemplate").html()),
                        width: 150
                    },
                    {
                        field: "ColumnName", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_COLUMN_NAME'),
                        template: kendo.template($("#columnNameTemplate").html()),
                        width: 150
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
                        field: "FundamentalType", title: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GRID_ATTRIBUTES_TYPE'),
                        template: kendo.template($("#fundamentalTypeTemplate").html()),
                        width: 128
                    }
                ]
            };
        }
    }

    $scope.categoryEditable = function(dataItem) {
        return MetadataService.CategoryEditable(dataItem);
    };

    $scope.isLatticeAttribute = function(dataItem) {
        return MetadataService.IsLatticeAttribute(dataItem);
    };

    $scope.categoryWarning = function(dataItem) {
        return $scope.isEmpty(dataItem.Category) && dataItem.ApprovedUsage !== "None" && dataItem.ApprovedUsage !== "Model";
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
                    { field: "DisplayName", operator: "contains", value: $scope.field },
                    { field: "ColumnName", operator: "contains", value: $scope.field }
                ]
            };
            orFilters.push(nameFiler);
        }

        var andFilters = [];
        if (!StringUtility.IsEmptyString($scope.category)) {
            andFilters.push({ field: "Category", operator: "eq", value: $scope.category });
        }

        var errorFilters = [];
        if ($scope.onlyShowErrorFields) {
            var columns = $scope.gridOptions.columns;
            for (var i = 0; i < columns.length; i++) {
                if (columns[i].field === "Category") {
                    errorFilters.push({
                        logic: "and",
                        filters: [
                            { field: "ApprovedUsage", operator: "neq", value: "None" },
                            { field: "ApprovedUsage", operator: "neq", value: "Model" },
                            { field: columns[i].field, operator: "eq", value: "" }
                        ]
                    });
                } else {
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

        $scope.showEditFieldsError = false;
        $scope.batchEdit = true;
    };

    $scope.saveClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        $scope.batchEdit = false;
        $scope.showEditFieldsError = false;
        $scope.saveInProgress = false;
    };

    $scope.discardChangesClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        $scope.batchEdit = false;
        $scope.showEditFieldsError = false;
        $scope.saveInProgress = false;
        $scope.discardAllChanges();
    };

    $scope.resetSettingsClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        if ($scope.saveInProgress) { return; }

        var editedData = getAllEditedData();
        if ((editedData != null && editedData.length > 0) || advancedSettingsFlagsChanged()) {
            DiscardEditFieldsModel.show($scope);
        } else {
            $scope.showEditFieldsError = true;
            $scope.editFieldsErrorMessage = "No fields changed. Cannot discard changes.";
        }
    };

    $scope.advancedSettingsClicked = function() {
        FieldMappingSettingsModal.show($scope.oneLeadPerDomain, $scope.includePersonalEmailDomains,
            $scope.useLatticeAttributes, $scope.enableTransformations, $scope.sourceType);
    };

    $scope.$on(NavUtility.MANAGE_FIELDS_ADVANCED_SETTINGS_EVENT, function(event, oneLeadPerDomain, includePersonalEmailDomains, useLatticeAttributes, enableTransformations) {
        $scope.oneLeadPerDomain = oneLeadPerDomain;
        $scope.includePersonalEmailDomains = includePersonalEmailDomains;
        $scope.enableTransformations = enableTransformations;

        if ($scope.useLatticeAttributes != useLatticeAttributes) {
            if (!useLatticeAttributes) {
                disableAllLatticeAttributes();
            } else {
                enableAllLatticeAttributes();
                loadFields();
            }
        }
        $scope.useLatticeAttributes = useLatticeAttributes;
    });

    function disableAllLatticeAttributes() {
        for (var i = 0; i < $scope.fields.length; i++) {
            var field = $scope.fields[i];
            if ($scope.isLatticeAttribute(field) && field.ApprovedUsage != 'None') {
                field.ApprovedUsage = 'None';
                $scope.indexToOldFieldsForListFieldsPage[i] = field;
                $scope.fields[i] = field;
            }
        }

        renderGrid($scope.fields);
    }

    function enableAllLatticeAttributes() {
        for (var index in $scope.indexToOldFieldsForListFieldsPage) {
            if ($scope.isLatticeAttribute($scope.indexToOldFieldsForListFieldsPage[index]) &&
                $scope.indexToOldFieldsForListFieldsPage[index].ApprovedUsage == 'None') {
                delete $scope.indexToOldFieldsForListFieldsPage[index];
            }
        }
    }

    function advancedSettingsFlagsChanged() {
        if ($scope.oneLeadPerDomain.toString() != Model.EventTableProvenance.Is_One_Lead_Per_Domain ||
            $scope.includePersonalEmailDomains.toString() == Model.EventTableProvenance.Exclude_Public_Domains ||
            $scope.useLatticeAttributes.toString() == Model.EventTableProvenance.Exclude_Propdata_Columns ||
            enableTransformationsChanged()) {
            return true;
        }
        return false;
    }

    function enableTransformationsChanged() {
        var transformationGroupName = Model.EventTableProvenance.Transformation_Group_Name ?
            Model.EventTableProvenance.Transformation_Group_Name : Model.ModelDetails.TransformationGroupName;

        if (transformationGroupName == "none" && $scope.enableTransformations) {
            return true;
        } else if (transformationGroupName != "none" && !$scope.enableTransformations) {
            return true;
        }
        return false;
    }

    $scope.remodelClicked = function($event) {
        if ($scope.saveInProgress) { return; }
        $scope.showEditFieldsError = false;
        if (allFieldsDisabled()) {
            $scope.showEditFieldsError = true;
            $scope.editFieldsErrorMessage = ResourceUtility.getString("SETUP_MANAGE_FIELDS_ALL_FIELDS_DISABLED_ERROR");
            return;
        }

        HealthService.checkSystemStatus().then(function() {
            $scope.remodel();
        });
    };

    $scope.remodel = function() {
        UpdateFieldsModal.show($scope.oneLeadPerDomain, $scope.includePersonalEmailDomains, $scope.useLatticeAttributes, $scope.enableTransformations, $scope.modelId,
            $scope.fields.concat($scope.fieldsNotDisplayed), Model.ModelDetails.DisplayName);
        $scope.saveInProgress = false;
    };

    function allFieldsDisabled() {
        for (var i = 0; i < $scope.fields.length; i++) {
            if ($scope.fields[i].ApprovedUsage.toLowerCase() != "none" ) {
                return false;
            }
        }

        return true;
    }

    function discardChangesOnPage() {
        $("#fieldsGrid").data("kendoGrid").cancelChanges();
        $scope.showEditFieldsError = false;
        $scope.batchEdit = false;
        $scope.oneLeadPerDomain = Model.EventTableProvenance.Is_One_Lead_Per_Domain == null ? false :
            Model.EventTableProvenance.Is_One_Lead_Per_Domain == "true";
        $scope.includePersonalEmailDomains = Model.EventTableProvenance.Exclude_Public_Domains == null ? true :
            Model.EventTableProvenance.Exclude_Public_Domains == "false";
        $scope.useLatticeAttributes = Model.EventTableProvenance.Exclude_Propdata_Columns == null ? true :
            Model.EventTableProvenance.Exclude_Propdata_Columns == "false";
        $scope.enableTransformations = (Model.EventTableProvenance.Transformation_Group_Name == "none" ||
            Model.ModelDetails.TransformationGroupName == "none") ? false : true;
        $scope.dirtyRows = {};

        $scope.indexToOldFieldsForListFieldsPage = {};
        $scope.indexToOldFieldsForSingleFieldPage = {};
    };

    $scope.discardAllChanges = function() {
        discardChangesOnPage();

        loadFields();
    };

    $scope.textboxClicked = function ($event) {
        $scope.showEditFieldsError = false;
        if ($scope.activeTextbox != $event.target) {
            $event.target.select();
            $scope.activeTextbox = $event.target;
        }
    };

    function getFieldIndexInFields(field) {
        for (var i = 0; i < $scope.fields.length; i++) {
            if ($scope.fields[i]['ColumnName'] == field['ColumnName']) {
                return i;
            }
        }
    }

    function hasFieldChanged(oldField, newField) {
        for (var i = 0; i < attributesEditable.length; i++) {
            if (oldField[attributesEditable[i]] != newField[attributesEditable[i]]) {
                return true;
            }
        }
        return false;
    }

    $scope.valueChanged = function ($event, dataItem, field, originalValue) {
        if ($event != null) {
            $event.preventDefault();
        }

        $scope.showEditFieldsError = false;
        var index = getFieldIndexInFields(dataItem);
        if ($scope.indexToOldFieldsForListFieldsPage[index] == null) {
            $scope.indexToOldFieldsForListFieldsPage[index] = $scope.fields[index];
        } else {
            // this means user has chagned the value but decided to change it back
            // and we should not be submit this field to clone & remodel
            if (! hasFieldChanged($scope.indexToOldFieldsForListFieldsPage[index], dataItem)) {
                delete $scope.indexToOldFieldsForListFieldsPage[index];
            }
        }
        if (dataItem[field] != originalValue) {
            $scope.fields[index] = dataItem;
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

    function getEditedDataOnPage() {
        var editedData = [];
        for (var index in $scope.indexToOldFieldsForListFieldsPage) {
            editedData.push($scope.fields[index]);
        }
        return editedData;
    }

    function getAllEditedData() {
        var editedData = getEditedDataOnPage();

        for (var index in $scope.indexToOldFieldsForSingleFieldPage) {
            editedData.push($scope.fields[index]);
        }

        return editedData;
    }

    $scope.isChanged = function (uid, field) {
        var row = $scope.dirtyRows[uid];
        return row && row[field] && row[field].dirty;
    };

    var attributesEditable = [ 'DisplayName', 'Description', 'ApprovedUsage', 'DisplayDiscretization', 'FundamentalType', 'StatisticalType' ];

    $scope.$on(SetupUtility.LOAD_FIELDS_EVENT, function (event, oldField, newField) {
        $scope.loading = true;

        if (hasFieldChanged(oldField, newField)) {
            var index = getFieldIndexInFields(newField);
            if ($scope.indexToOldFieldsForSingleFieldPage[index] != null
                && ! hasFieldChanged(newField, $scope.indexToOldFieldsForSingleFieldPage[index])) {
                // this is when user changed the field value and then decide to change it back
                delete $scope.indexToOldFieldsForSingleFieldPage[index];
            } else {
                $scope.indexToOldFieldsForSingleFieldPage[index] = oldField;
            }

            $scope.fields[index] = newField;
        }

        renderGrid($scope.fields);
        $scope.loading = false;
    });

    $scope.isEmpty = function(value) {
        return value == null || value === '';
    };
});