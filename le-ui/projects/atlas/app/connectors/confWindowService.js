import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import ObjectUtilities from "common/app/utilities/ObjectUtilities";
import {
	MARKETO,
	SALESFORCE,
	ELOQUA,
	LINKEDIN,
	FACEBOOK,
	OUTREACH
} from "./connectors.service";

// export const solutionInstanceConfig = {
// 	orgType: null,
// 	id: null,
// 	accessToken: null,
// 	registerLookupIdMap: null,
// 	fieldMapping: null,
// 	system: null
// };

export const FIELD_MAPPING = "external_field_mapping";
const EMAIL = "email";

class ConfWindowService {
	constructor() {
		this.solutionInstanceConfig = {
			orgType: null,
			id: null,
			accessToken: null,
			registerLookupIdMap: null,
			fieldMapping: null,
			system: null
		};
		this.lookupIdMapRegistered = false;
	}
	getSolutionInstanceConfig() {
		return this.solutionInstanceConfig;
	}
	getTrayAuthValues(solutionInstanceId) {
		let observer = new Observer(
			response => {
				if (response.data) {
					console.log(response.data);
					var authValues = response.data.solutionInstance.authValues;
					var configValues =
						response.data.solutionInstance.configValues;
					var authenticationExternalId =
						"external_" +
						this.solutionInstanceConfig.orgType.toLowerCase() +
						"_authentication";
					// console.log(authenticationExternalId);
					var externalAuthentication = authValues.filter(function(
						authValue
					) {
						return authValue.externalId == authenticationExternalId;
					});
					var trayAuthenticationId = externalAuthentication[0].authId;
					var authentication = response.data.authentications.filter(
						function(auth) {
							return auth.node.id == trayAuthenticationId;
						}
					);

					var httpClientAuth = authValues.filter(function(authValue) {
						return (
							authValue.externalId ==
							"external_http_client_authentication"
						);
					});

					this.registerLookupIdMap(
						trayAuthenticationId,
						authentication[0].node.name,
						this.getLookupIdMapConfiguration(
							authentication[0],
							configValues
						)
					);
					if (
						this.solutionInstanceConfig.orgType == LINKEDIN ||
						this.solutionInstanceConfig.orgType == FACEBOOK
					) {
						this.updateSolutionInstance(
							response.data.solutionInstance.id,
							response.data.solutionInstance.name,
							{
								externalId:
									"external_http_client_authentication",
								authId: trayAuthenticationId
							}
						);
					} else {
						this.updateSolutionInstance(
							response.data.solutionInstance.id,
							response.data.solutionInstance.name
						);
					}
					httpService.unsubscribeObservable(observer);
				}
			},
			error => {
				console.error(
					"Error retrieving authValues: " + JSON.stringify(response)
				);
			}
		);
		httpService.get(
			"/tray/solutionInstances/" + solutionInstanceId,
			observer,
			{ useraccesstoken: this.solutionInstanceConfig.accessToken }
		);
	}

	guidGenerator() {
		var S4 = function() {
			return (((1 + Math.random()) * 0x10000) | 0)
				.toString(16)
				.substring(1);
		};
		return (
			S4() +
			S4() +
			"-" +
			S4() +
			"-" +
			S4() +
			"-" +
			S4() +
			"-" +
			S4() +
			S4() +
			S4()
		);
	}

	getTrayAuthValues(solutionInstanceId) {
		let observer = new Observer(
			response => {
				if (response.data) {
					console.log(response.data);
					var authValues = response.data.solutionInstance.authValues;
					var configValues =
						response.data.solutionInstance.configValues;
					var authenticationExternalId =
						"external_" +
						this.solutionInstanceConfig.orgType.toLowerCase() +
						"_authentication";
					console.log(authenticationExternalId);
					var externalAuthentication = authValues.filter(function(
						authValue
					) {
						return authValue.externalId == authenticationExternalId;
					});
					var trayAuthenticationId = externalAuthentication[0].authId;
					var authentication = response.data.authentications.filter(
						function(auth) {
							return auth.node.id == trayAuthenticationId;
						}
					);

					var httpClientAuth = authValues.filter(function(authValue) {
						return (
							authValue.externalId ==
							"external_http_client_authentication"
						);
					});

					this.registerLookupIdMap(
						trayAuthenticationId,
						authentication[0].node.name,
						this.getLookupIdMapConfiguration(
							authentication[0],
							configValues
						)
					);
					if (
						this.solutionInstanceConfig.orgType == LINKEDIN ||
						this.solutionInstanceConfig.orgType == FACEBOOK
					) {
						this.updateSolutionInstance(
							response.data.solutionInstance.id,
							response.data.solutionInstance.name,
							{
								externalId:
									"external_http_client_authentication",
								authId: trayAuthenticationId
							}
						);
					} else {
						this.updateSolutionInstance(
							response.data.solutionInstance.id,
							response.data.solutionInstance.name
						);
					}
					httpService.unsubscribeObservable(observer);
				}
			},
			error => {
				console.error(
					"Error retrieving authValues: " + JSON.stringify(response)
				);
			}
		);
		httpService.get(
			"/tray/solutionInstances/" + solutionInstanceId,
			observer,
			{ useraccesstoken: this.solutionInstanceConfig.accessToken }
		);
	}

	registerLookupIdMap(
		trayAuthenticationId,
		trayAuthenticationName,
		lookupIdMapConfiguration
	) {
		let observer = new Observer(
			response => {
				console.log("response", response.data);
				if (response.data && response.data.configId) {
					httpService.unsubscribeObservable(observer);
					this.lookupIdMapRegistered = true;
					console.log("Set lookupIdMapRegistered as true");
				} else {
					console.log("response", response);
				}
			},
			error => {
				console.error(
					"Error registering lookupIdMap ",
					error ? error : ""
				);
			}
		);

		console.log(lookupIdMapConfiguration);

		var lookupIdMap = {
			orgId: lookupIdMapConfiguration.orgId,
			orgName: lookupIdMapConfiguration.orgName,
			accountId: lookupIdMapConfiguration.accountId || null,
			externalSystemType:
				lookupIdMapConfiguration.externalSystemType || "MAP",
			externalSystemName: this.solutionInstanceConfig.orgType,
			externalAuthentication: {
				solutionInstanceId: this.solutionInstanceConfig.id,
				trayWorkflowEnabled: true,
				trayAuthenticationId: trayAuthenticationId
			},
			exportFieldMappings: this.constructExportFieldMappings()
		};

		console.log(JSON.stringify(lookupIdMap));

		if (this.lookupIdMapRegistered == false) {
			httpService.post(
				"/pls/lookup-id-mapping/register",
				lookupIdMap,
				observer
			);
		} else {
			console.error("Prevent duplicate lookupIdMaps ", lookupIdMap);
		}
	}

	constructExportFieldMappings() {
		if (
			this.solutionInstanceConfig.orgType != MARKETO ||
			!this.solutionInstanceConfig.fieldMapping
		) {
			return [];
		}
		return this.solutionInstanceConfig.fieldMapping.map(mapping => {
			return {
				sourceField: this.parseLatticeField(mapping.field_left),
				destinationField: mapping.field_right,
				overwriteValue: false
			};
		});
	}

	parseLatticeField(field) {
		return field.replace("CONTACT:", "");
	}

	updateSolutionInstance(
		solutionInstanceId,
		solutionInstanceName,
		authValues
	) {
		let observer = new Observer(
			response => {
				if (response.data) {
					httpService.unsubscribeObservable(observer);
				}
			},
			error => {
				console.error(
					"Error updating solution instance: " +
						JSON.stringify(response)
				);
			}
		);
		httpService.put(
			"/tray/solutionInstances/" + solutionInstanceId,
			{
				solutionInstanceName: solutionInstanceName,
				authValues: authValues
			},
			observer,
			{ useraccesstoken: this.solutionInstanceConfig.accessToken }
		);
	}

	updateSystem() {
		let observer = new Observer(
			response => {
				if (response.data && response.data.name) {
					httpService.unsubscribeObservable(observer);
				} else {
					console.log("response", response);
				}
			},
			error => {
				console.error("Error registering lookupIdMap ", error);
			}
		);

		var lookupIdMap = this.solutionInstanceConfig.system;
		lookupIdMap.exportFieldMappings = this.constructExportFieldMappings();
		console.log("Update LookupIdMap:" + lookupIdMap);

		httpService.put(
			"/pls/lookup-id-mapping/config/" + lookupIdMap.configId,
			lookupIdMap,
			observer
		);
	}

	verifyFieldMapping(fieldMappingValues, errors, externalId) {
		console.log(fieldMappingValues);
		switch (this.solutionInstanceConfig.orgType) {
			case MARKETO:
				var marketoFields = new Set();
				if (!fieldMappingValues || fieldMappingValues.length == 0) {
					errors[externalId] = `No fields have been mapped.`;
					break;
				}
				fieldMappingValues.some(function(mapping) {
					if (marketoFields.has(mapping.field_right)) {
						errors[externalId] = `The Marketo field ${
							mapping.field_right
						} has been mapped multiple times.`;
						return;
					}
					let tmp = mapping.field_left;
					if (
						typeof tmp === "object" &&
						ObjectUtilities.isEmpty(tmp)
					) {
						errors[externalId] = `Lattice field cannot be blank.`;
						return;
					}
					marketoFields.add(mapping.field_right);
				});
				if (!marketoFields.has(this.EMAIL)) {
					errors[
						externalId
					] = `The email field in Marketo is required.`;
					break;
				}
				this.solutionInstanceConfig.fieldMapping = fieldMappingValues;
				break;
		}
	}

	getLookupIdMapConfiguration(authentication, configValues) {
		var trayAuthenticationName =
			authentication && authentication.node
				? authentication.node.name
				: this.solutionInstanceConfig.orgType +
				  "_" +
				  new Date().getTime();
		console.log(authentication);
		switch (this.solutionInstanceConfig.orgType) {
			case MARKETO:
				var customFields = JSON.parse(authentication.node.customFields);
				return {
					orgId:
						customFields && customFields.identification
							? customFields.identification.marketo_org_id
							: this.guidGenerator(),
					orgName: trayAuthenticationName,
					externalSystemType: "MAP"
				};
			case LINKEDIN:
			case FACEBOOK:
				var externalAdAccount = configValues.filter(function(
					configValue
				) {
					return configValue.externalId == "external_ad_account";
				});
				var adAccount =
					externalAdAccount.length > 0
						? externalAdAccount[0].value.replace(/['"]+/g, "")
						: this.guidGenerator();
				return {
					orgId: adAccount,
					orgName: trayAuthenticationName,
					externalSystemType: "ADS"
				};
			case OUTREACH:
			default:
				return {
					orgId: this.guidGenerator(),
					orgName: trayAuthenticationName,
					externalSystemType: "MAP"
				};
		}
	}
}

const instance = new ConfWindowService();

export default instance;
