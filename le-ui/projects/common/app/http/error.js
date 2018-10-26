export const NOT_FOUND = 404;

export default class Error {
  constructor(status = 404, msg = "", fullMessage = "") {
    this.status = status;
    this.msg = msg;
    this.fullMessage = fullMessage;
  }

  getStatus() {
    return this.status;
  }
  getMsg() {
    return this.msg;
  }
  getFullMessage() {
    return this.fullMessage;
  }
}
