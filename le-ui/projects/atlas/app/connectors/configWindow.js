import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import { MARKETO, SALESFORCE, ELOQUA } from './connectors.service';

export const solutionInstanceConfig = {
    orgType: null,
    id: null,
    accessToken: null,
    registerLookupIdMap: null,
    fieldMapping: null,
    system: null
};

const FIELD_MAPPING = 'external_field_mapping';
const EMAIL = "email";

export const openConfigWindow = () => {
    // Must open window from user interaction code otherwise it is likely
    // to be blocked by a popup blocker:
    var w = 500;
    var h = 600;
    var left = (window.screen.width / 2) - ((w / 2) + 10);
    var top = (window.screen.height / 2) - ((h / 2) + 50);
    const configWindow = window.open(
        undefined,
        '_blank',
        'width='+w+',height='+h+',scrollbars=no,top='+top+', left='+left
    );
    

    // Listen to popup messages
    let configFinished = false;
    let lookupIdMapRegistered = false;
    const onmessage = e => {
        if (e.data.type === 'tray.configPopup.error') {
            // Handle popup error message
            alert(`Error: ${e.data.err}`);
        }
        if (e.data.type === 'tray.configPopup.cancel') {
            configWindow.close();
        }
        if (e.data.type === 'tray.configPopup.finish') {
            
            // Handle popup finish message
            if (solutionInstanceConfig.registerLookupIdMap == true) {
                console.log("Register new system");
                if (solutionInstanceConfig.id) {
                    // get Tray auth values
                    // create lookup id map
                    // enable solution instance
                    getTrayAuthValues(solutionInstanceConfig.id);
                } else {
                    alert('Error: Solution instance id is not defined');
                }
            } else {
                console.log("Update system");
                updateSystem();
            }
            configFinished = true;
            configWindow.close();
        }
        if (e.data.type === 'tray.configPopup.validate') {
            // Return validation in progress
            configWindow.postMessage({
                type: 'tray.configPopup.client.validation',
                data: {
                    inProgress: true,
                }
            }, '*');

            setTimeout(() => {
                    // Add errors to all inputs
                    console.log(e.data.data);
                    const errors = e.data.data.visibleValues.reduce(
                        (errors, externalId) => {
                            console.log(`Visible ${externalId} value:`, e.data.data.configValues[externalId]);
                            if (externalId == FIELD_MAPPING) {
                                verifyFieldMapping(e.data.data.configValues[externalId], errors, externalId);
                            }
                            return errors;
                        },
                        {}
                    );

                    // Return validation
                    configWindow.postMessage({
                        type: 'tray.configPopup.client.validation',
                        data: {
                            inProgress: false,
                            errors: errors,
                        }
                    }, '*');
                },
                2000
            );
        }
    };
    window.addEventListener('message', onmessage);

    // Check if popup window has been closed before finishing the configuration.
    // We use a polling function due to the fact that some browsers may not
    // display prompts created in the beforeunload event handler.

    const CHECK_TIMEOUT = 1000;
    const checkWindow = () => {
        if (configWindow.closed) {
            // Handle popup closing
            if (configFinished) {
                console.log('Configuration finished');
            }
            window.removeEventListener('message', onmessage);
        } else {
            setTimeout(checkWindow, CHECK_TIMEOUT);
        }
    }

    function guidGenerator() {
        var S4 = function() {
           return (((1+Math.random())*0x10000)|0).toString(16).substring(1);
        };
        return (S4()+S4()+"-"+S4()+"-"+S4()+"-"+S4()+"-"+S4()+S4()+S4());
    }

    function getTrayAuthValues(solutionInstanceId) {
        let observer = new Observer(
            response => {
                if (response.data) {
                    var authValues = response.data.solutionInstance.authValues;
                    var authenticationExternalId = "external_" + solutionInstanceConfig.orgType.toLowerCase() + "_authentication";
                    console.log(authenticationExternalId);
                    var externalAuthentication = authValues.filter(function(authValue) {
                        return authValue.externalId == authenticationExternalId;
                    });
                    var trayAuthenticationId = externalAuthentication[0].authId;
                    var authenticationName = response.data.authentications.filter(function(auth) {
                        return auth.node.id == trayAuthenticationId;
                    });
                    registerLookupIdMap(trayAuthenticationId, authenticationName[0].node.name);
                    updateSolutionInstance(response.data.solutionInstance.id, response.data.solutionInstance.name);
                    httpService.unsubscribeObservable(observer);
                }
            },
            error => {
                console.error("Error retrieving authValues: " + JSON.stringify(response));
            }
        );
        httpService.get('/tray/solutionInstances/' + solutionInstanceId, observer, {useraccesstoken: solutionInstanceConfig.accessToken});
    }

    function registerLookupIdMap(trayAuthenticationId, trayAuthenticationName) {
        let observer = new Observer(
            response => {
                console.log('response', response.data);
                if (response.data && response.data.configId) {
                    httpService.unsubscribeObservable(observer);
                    lookupIdMapRegistered = true;
                    console.log("Set lookupIdMapRegistered as true");
                } else {
                    console.log("response", response);
                }
            },
            error => {
                console.error('Error registering lookupIdMap ', error ? error : '');
            }
        );

        var lookupIdMap = {
            orgId: guidGenerator(),
            orgName: trayAuthenticationName ? trayAuthenticationName : solutionInstanceConfig.orgType + '_' + (new Date()).getTime(),
            externalSystemType: "MAP",
            externalSystemName: solutionInstanceConfig.orgType,
            externalAuthentication: {
                solutionInstanceId: solutionInstanceConfig.id,
                trayWorkflowEnabled: true,
                trayAuthenticationId: trayAuthenticationId
            },
            exportFieldMappings: constructExportFieldMappings()
        };

        console.log(JSON.stringify(lookupIdMap));

        if (lookupIdMapRegistered == false) {
            httpService.post('/pls/lookup-id-mapping/register', lookupIdMap, observer);
        } else {
            console.error('Prevent duplicate lookupIdMaps ', lookupIdMap);
        }
    }

    function constructExportFieldMappings() {
        if (solutionInstanceConfig.orgType != MARKETO || !solutionInstanceConfig.fieldMapping) {
            return [];
        }
        return solutionInstanceConfig.fieldMapping.map(mapping => {
            return {
                sourceField: parseLatticeField(mapping.field_left),
                destinationField: mapping.field_right,
                overwriteValue: false
            };
        })
    }

    function parseLatticeField(field) {
        return field.replace('CONTACT:', '');
    }

    function updateSolutionInstance(solutionInstanceId, solutionInstanceName) {
        let observer = new Observer(
            response => {
                if (response.data) {
                    httpService.unsubscribeObservable(observer);
                }
            },
            error => {
                console.error("Error updating solution instance: " + JSON.stringify(response));
            }
        );
        httpService.put('/tray/solutionInstances/' + solutionInstanceId, {solutionInstanceName: solutionInstanceName}, observer, {useraccesstoken: solutionInstanceConfig.accessToken});
    }

    function updateSystem() {
        let observer = new Observer(
            response => {
                if (response.data && response.data.name) {
                    httpService.unsubscribeObservable(observer);
                } else {
                    console.log("response", response);
                }
            },
            error => {
                console.error('Error registering lookupIdMap ', error);
            }
        );

        var lookupIdMap = solutionInstanceConfig.system;
        lookupIdMap.exportFieldMappings = constructExportFieldMappings();
        console.log("Update LookupIdMap:" + lookupIdMap);

        httpService.put('/pls/lookup-id-mapping/config/' + lookupIdMap.configId, lookupIdMap, observer);
    }

    function verifyFieldMapping(fieldMappingValues, errors, externalId) {
        console.log(fieldMappingValues);
        switch(solutionInstanceConfig.orgType) {
          case MARKETO:
            var marketoFields = new Set();
            if (!fieldMappingValues || fieldMappingValues.length == 0) {
                errors[externalId] = `No fields have been mapped.`;
                break;
            }
            fieldMappingValues.some(function(mapping) {
                if (marketoFields.has(mapping.field_right)) {
                    errors[externalId] = `The Marketo field ${mapping.field_right} has been mapped multiple times.`;
                    return;
                }
                if (isEmptyObject(mapping.field_left)) {
                    errors[externalId] = `Lattice field cannot be blank.`;
                    return;  
                }
                marketoFields.add(mapping.field_right);
            });
            if (!marketoFields.has(EMAIL)) {
                errors[externalId] = `The email field in Marketo is required.`;
                break;
            }
            solutionInstanceConfig.fieldMapping = fieldMappingValues;
            break;
        }

    }

    function isEmptyObject(obj) {
        return obj === Object(obj) && Object.entries(obj).length === 0;
    }


    checkWindow();

    return configWindow;
};