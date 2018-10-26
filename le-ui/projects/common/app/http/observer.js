/**
 * Class for the creation of the Observer
 */
export default class Observer {
  constructor(next, error) {
      this.name = new Date().getMilliseconds();
    if (next) {
      this.next = next;
    } else {
      this.next = response => {
        console.log("Observable with next method not implemented", response);
      };
    }
    if (error) {
      this.error = error;
    } else {
      this.error = error => {
        // console.log('Observable with error method not implemented', error);
      };
    }
  }

  getName(){
      return this.name;
  }
}
