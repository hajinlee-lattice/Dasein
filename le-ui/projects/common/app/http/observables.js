export default class Observables {
    constructor(){
        this.observables = {};
    }

    addObservable(name, observable){
        if(!this.observables[name]){
            this.observables[name] = observable;
        }
    }
    removeObservable(name){ 
        if(this.observables[name]){
            this.observables[name].unsubscribe();
            delete this.observables[name];
        }
    }
}