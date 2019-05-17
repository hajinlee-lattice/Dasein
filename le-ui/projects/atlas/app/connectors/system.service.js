class SystemService {
    constructor() {
        if (!SystemService.instance) {
            SystemService.instance = this;
        }

        return SystemService.instance;
    }
    getValueDateFormatted(longValue) {
        if (longValue && longValue != null) {
            var options = {
                year: "numeric",
                month: "2-digit",
                day: "2-digit"
            };
            var formatted = new Date(longValue);

            var ret = "err";
            try {
                ret = formatted.toLocaleDateString(
                    "en-US",
                    options
                );
            } catch (e) {
                console.log(e);
            }

            return ret;
        } else {
            return '-';
        }
    }
    getSystemStatus(system) {
        switch (system.isRegistered) {
            case true:
                return 'Connected';
            case false:
                return 'Disconnected';
        }
    }
    canHaveAccountId(system) {
        switch (system.externalSystemType) {
            case 'MAP':
            case 'FILE_SYSTEM':
                return false;
            default:
                return true;
        }
    }
    canEditMapping(system) {
        console.log(system);
        switch (system.externalSystemName) {
            case 'Marketo':
                return true;

            default:
                return this.canHaveAccountId(system);
        }
        // return true;
        // switch(system)
    }

    getNewToken() {

    }
}

const instance = new SystemService();
Object.freeze(instance);

export default instance;

