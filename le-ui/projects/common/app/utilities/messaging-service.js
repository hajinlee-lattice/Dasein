import { Observable } from "../../network.vendor";
import Observables from "./observables";


let observables;


const init = () => {
  if (!observables) {
    observables = new Observables();
  }
};

/**
 * http service
 */
const messagingService = {
    
};
init();

export default messagingService;
