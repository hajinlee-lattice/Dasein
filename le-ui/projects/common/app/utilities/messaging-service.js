import { Subject } from "../../network.vendor";


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

  unsubscribeObservable: observer =>{
    observables.removeObservable(observer.getName());
  },
  addSubscriber: observer =>{
    subject.subscribe(observer);
  },
  removeSubscriber: observer => {
    
  },
  sendMessage: message => {
    console.log('SENDING');
    subject.next(message);
  }
    
};
init();

export default messagingService;
