import { Subject } from '../../network.vendor';

const init = () => {
    if (!window['subject']) {
        window['subject'] = new Subject();
    }
};

/**
 * http service
 */
const messagingService = {
    unsubscribeObservable: observer => {
        observables.removeObservable(observer.getName());
    },
    addSubscriber: observer => {
        window['subject'].subscribe(observer);
    },
    removeSubscriber: observer => { },
    sendMessage: message => {
        window['subject'].next(message);
    }
};
init();

export const MessagingFactory = () => {
    return {
        subscribe: observer => {
            messagingService.addSubscriber(observer);
        }
    };
};

export default messagingService;
