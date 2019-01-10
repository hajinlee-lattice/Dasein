import { Subject } from '../../network.vendor';

let subject;

const init = () => {
    if (!subject) {
        subject = new Subject();
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
        // console.log('ADD SUB TO MESSAGING');
        subject.subscribe(observer);
    },
    removeSubscriber: observer => {},
    sendMessage: message => {
        // console.log('SENDING');
        subject.next(message);
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
