import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";

export const solutionInstanceConfig = {
    id: null
};

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
            if (solutionInstanceConfig.id) {
                let observer = new Observer(
                    response => {
                        if (response.data && response.data.name) {
                            httpService.unsubscribeObservable(observer);
                        } else {
                            console.log("response", response);
                        }
                    },
                    error => {
                        console.error('ERROR ', error);
                    }
                );

                var lookupIdMap = {
                    orgId: guidGenerator(),
                    orgName: "Marketo_" +  (new Date()).getTime(),
                    externalSystemType: "MAP",
                    externalSystemName: "Marketo",
                    externalAuthentication: {
                        solutionInstanceId: solutionInstanceConfig.id,
                        trayWorkflowEnabled: false
                    }
                };

                console.log("LookupIdMap: " + JSON.stringify(lookupIdMap));

                httpService.post('/pls/lookup-id-mapping/register', lookupIdMap, observer);
            } else {
                alert('Error: Solution instance id is not defined');
            }
            configFinished = true;
            configWindow.close();
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

    checkWindow();

    return configWindow;
};