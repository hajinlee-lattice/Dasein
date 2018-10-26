export const SUCCESS = 200;

export default class Response {
  constructor(status = 200, msg = "", data = []) {
    this.status = status;
    this.msg = msg;
    this.data = data;
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
