import httpService from "common/app/http/http-service";

class PAJobsService {
    constructor() {
        if (!PAJobsService.instance) {
            PAJobsService.instance = this;
        }
        return PAJobsService.instance;
    }
    // url, body, observer, headers
    cancelAction(pid, observer){

        httpService.post(
            "/pls/actions/cancel?actionPid="+pid,
            {},
            observer
          );
    }
    unregister(observer) {
        httpService.unsubscribeObservable(observer);
    }
    
}

const instance = new PAJobsService();
Object.freeze(instance);

export default instance;

export const NGPAJobsService = () => {
    return {
        unregister : observer => {
            instance.unregister(observer);
        },
        cancelAction: (pid,observer) => {
            instance.cancelAction(pid,observer);
        }
    };
};

