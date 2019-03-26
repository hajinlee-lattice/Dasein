class NgState {
  constructor() {
      if (!NgState.instance) {
          NgState.instance = this;
          this._$state = { };
      }

      return NgState.instance;
  }

  setAngularState($state){
    if(!this._$state.state){
      this._$state['state'] = $state;
    }
  }

  getAngularState(){
    return this._$state.state;
  }

}

const instance = new NgState();
Object.freeze(instance);

export default instance;
