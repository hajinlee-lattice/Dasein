import {
	actions,
	reducer
} from "../../templates/multiple/multipletemplates.redux";
import { store, injectAsyncReducer } from "store";

angular
	.module("lp.import.wizard.matchtoaccounts", [])
	.controller("ImportWizardMatchToAccounts", function(
		$state,
		$stateParams,
		$scope,
		$timeout,
		ResourceUtility,
		ImportWizardStore,
		FieldDocument,
		UnmappedFields,
		MatchingFields,
		Banner,
		FeatureFlagService
	) {
		var vm = this;
		var alreadySaved = ImportWizardStore.getSavedDocumentFields(
			$state.current.name
		);
		// console.log(alreadySaved)
		if (alreadySaved) {
			FieldDocument.fieldMappings = alreadySaved;
			// vm.updateAnalysisFields();
		}
		var makeList = function(object) {
			var list = [];
			for (var i in object) {
				list.push(object[i]);
			}
			return list;
		};

		var matchingFieldsList = makeList(MatchingFields),
			ignoredFieldLabel = "-- Unmapped Field --",
			noFieldLabel = "-- No Fields Available --",
			entityMatchEnabled = ImportWizardStore.entityMatchEnabled;

		angular.extend(vm, {
			state: ImportWizardStore.getAccountIdState(),
			fieldMapping: {},
			fieldMappings: FieldDocument.fieldMappings,
			fieldMappingsMap: {},
			AvailableFields: [],
			unavailableFields: [],
			idFieldMapping: {
				userField: "Id",
				mappedField: "Id",
				fieldType: "TEXT",
				mappedToLatticeField: true
			},
			mappedFieldMap: {
				contact: entityMatchEnabled ? "CustomerContactId" : "ContactId",
				account: entityMatchEnabled ? "CustomerAccountId" : "AccountId",
				contactUniqueId: {}
			},
			UnmappedFieldsMappingsMap: {},
			savedFields: ImportWizardStore.getSaveObjects($state.current.name),
			initialMapping: {},
			keyMap: {},
			saveMap: {},
			entityMatchEnabled: ImportWizardStore.entityMatchEnabled,
			ignoredFields: FieldDocument.ignoredFields || [],
			ignoredFieldLabel: ignoredFieldLabel,
			matchingFieldsList: angular.copy(matchingFieldsList),
			matchingFields: MatchingFields,
			systemName: "",
			systems: []
		});

		vm.init = function() {
			injectAsyncReducer(store, "multitemplates.matchaccount", reducer);
			this.unsubscribe = store.subscribe(() => {
				const data = store.getState()["multitemplates.matchaccount"];
				// console.log("DATA ", data.systems);
				vm.systems = data.systems;
				//vm.systems = [{ displayName: '-- Select System --', name: 'select'},{name: 't4', displayName: 'Test 4'}, {name: 't5', displayName: 'Test 5'}];
			});
			const tmp = store.getState()["multitemplates.matchaccount"];
			actions.fetchSystems({}, tmp.systemSelected);
			// actions.fetchSystems({});
			let validationStatus = ImportWizardStore.getValidationStatus();
			let banners = Banner.get();
			if (validationStatus && banners.length == 0) {
				let messageArr = validationStatus.map(function(error) {
					return error["message"];
				});
				Banner.error({ message: messageArr });
			}

			vm.UnmappedFields = UnmappedFields;

			ImportWizardStore.setUnmappedFields(UnmappedFields);
			ImportWizardStore.setValidation(
				"matchtoaccounts",
				vm.entityMatchEnabled
			);

			var userFields = [];
			vm.fieldMappings.forEach(function(fieldMapping, index) {
				vm.fieldMappingsMap[fieldMapping.mappedField] = fieldMapping;
				if (fieldMapping.mappedField == vm.mappedFieldMap.contact) {
					vm.mappedFieldMap.contactUniqueId = Object.assign(
						{},
						fieldMapping
					);
				}
				if (userFields.indexOf(fieldMapping.userField) === -1) {
					userFields.push(fieldMapping.userField);
					vm.AvailableFields.push(fieldMapping);
				}
				for (var i in vm.mappedFieldMap) {
					if (fieldMapping.mappedField == vm.mappedFieldMap[i]) {
						vm.fieldMapping[i] = fieldMapping.userField;
					}
				}
			});
			if (vm.savedFields) {
				vm.savedFields.forEach(function(fieldMapping, index) {
					vm.saveMap[fieldMapping.originalMappedField] = fieldMapping;

					vm.fieldMappingsMap[
						fieldMapping.mappedField
					] = fieldMapping;
					if (userFields.indexOf(fieldMapping.userField) === -1) {
						userFields.push(fieldMapping.userField);
						vm.AvailableFields.push(fieldMapping);
					}
					for (var i in vm.mappedFieldMap) {
						if (fieldMapping.mappedField == vm.mappedFieldMap[i]) {
							vm.fieldMapping[i] = fieldMapping.userField;
						}
					}
				});
			}
			vm.AvailableFields = vm.AvailableFields.filter(function(item) {
				return item.userField && typeof item.userField !== "object";
			});
		};

		/**
		 * NOTE: The delimiter could cause a problem if the column name has : as separator
		 * @param {*} string
		 * @param {*} delimiter
		 */
		var makeObject = function(string, delimiter) {
			var delimiter = delimiter || "^/",
				string = string || "",
				pieces = string.split(delimiter);

			return {
				mappedField: pieces[0],
				userField: pieces[1] === "" ? fallbackUserField : pieces[1] // allows unmapping
			};
		};

		vm.getMapped = mapping => {
			var mapped = [];
			vm.unavailableFields = [];
			for (var i in mapping) {
				var key = i,
					userFieldObject =
						typeof mapping[key] === "object"
							? makeObject(mapping[key].userField)
							: {
									userField: mapping[key],
									mappedField: vm.mappedFieldMap[key]
							  },
					map = {
						userField: userFieldObject.userField,
						mappedField: userFieldObject.mappedField,
						// removing the following 3 lines makes it update instead of append
						originalUserField: vm.saveMap[vm.mappedFieldMap[key]]
							? vm.saveMap[vm.mappedFieldMap[key]]
									.originalUserField
							: vm.keyMap[vm.mappedFieldMap[key]],
						originalMappedField: vm.saveMap[vm.mappedFieldMap[key]]
							? vm.saveMap[vm.mappedFieldMap[key]]
									.originalMappedField
							: vm.mappedFieldMap[key],
						append: false
					};
				mapped.push(map);
				if (userFieldObject && userFieldObject.userField) {
					vm.unavailableFields.push(userFieldObject.userField);
				}
			}
			return mapped;
		};

		vm.changeLatticeField = function(mapping, form) {
			let mapped = vm.getMapped(mapping);
			if (vm.isMultipleTemplates()) {
				vm.changeSystem(mapped);
			}
			ImportWizardStore.setSaveObjects(mapped, $state.current.name);
			// console.log(mapped);
			vm.checkValid(form);
		};

		vm.changeSystem = mapped => {
			mapped.forEach(item => {
				if (item.mappedField == "CustomerAccountId") {
					item.SystemName = vm.systemName;
					item.IdType = "Account";
				} else if (item.mappedField == "CustomerContactId") {
					item.mapToLatticeId =
						vm.mappedFieldMap.contactUniqueId.mapToLatticeId;
					item.IdType = "Contact";
				} else {
					item.SystemName = null;
					item.IdType = null;
				}
			});
		};

		vm.changeMatchingFields = function(mapping, form) {
			vm.changeLatticeField(mapping, form);
		};

		vm.checkFieldsDelay = function(form) {
			var mapped = [];
			$timeout(function() {
				for (var i in vm.fieldMapping) {
					var key = i,
						userField = vm.fieldMapping[key];

					vm.keyMap[vm.mappedFieldMap[key]] = userField;
					vm.initialMapping[key] = userField;
					if (userField) {
						vm.unavailableFields.push(userField);
					}
				}
			}, 1);
		};

		vm.checkMatchingFieldsDelay = function(form) {
			$timeout(function() {
				for (var i in vm.fieldMapping) {
					var key = i,
						userField = vm.fieldMapping[key];

					vm.keyMap[key] = userField;
					vm.initialMapping[key] = userField;

					var fieldMapping = vm.fieldMapping[i],
						fieldObj = makeObject(fieldMapping.userField);

					vm.unavailableFields.push(fieldObj.userField);
				}
			}, 1);
		};

		vm.checkValidDelay = function(form) {
			$timeout(function() {
				vm.checkValid(form);
			}, 1);
		};

		vm.checkValid = function(form) {
			ImportWizardStore.setValidation("matchtoaccounts", vm.form.$valid);
		};

		vm.isMultipleTemplates = () => {
			var flags = FeatureFlagService.Flags();
			var multipleTemplates = FeatureFlagService.FlagIsEnabled(
				flags.ENABLE_MULTI_TEMPLATE_IMPORT
			);
			return multipleTemplates;
		};

		vm.updateSystem = () => {
			vm.changeLatticeField(vm.fieldMapping, vm.form);
		};

		vm.init();
	});
