export const SUCCESS = 200;

export default class Response {
  constructor(xhrResponse, status = 200, msg = "", data = []) {
    this.xhrResponse = xhrResponse;
    this.status = status;
    this.msg = msg;
    this.data = data;
  }

  getXhrResponse(){
    return this.xhrResponse;
  }
  getStatus() {
    return this.status;
  }
  getMsg() {
    return this.msg;
  }
  getData() {
    return this.data;
  }
}
