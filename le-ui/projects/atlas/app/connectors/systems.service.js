export const MARKETO = 'Marketo';
export const SALESFORCE = 'Salesforce';
export const ELOQUA = 'Eloqua';
const CRM = {
    externalSystemType: 'CRM',
    externalSystemName : SALESFORCE
};
const MAP = {
    externalSystemType: 'MAP',
    externalSystemName : ELOQUA
};
class SystemsService {
    constructor() {
        if (!SystemsService.instance) {
            SystemsService.instance = this;
        }

        return SystemsService.instance;
    }
    cleanupLookupId(systemsList){
        if (systemsList) {
            systemsList.forEach(system => {
                if(system.externalSystemName == null){
                    switch(system.externalSystemType){
                        case CRM.externalSystemType:
                            system.externalSystemName = CRM.externalSystemName;
                        break;
                        case MAP.externalSystemType:
                            system.externalSystemName = MAP.externalSystemName;
                        break;
                    }
                }
            });
        }

    }
    
}

const instance = new SystemsService();
Object.freeze(instance);

export default instance;

