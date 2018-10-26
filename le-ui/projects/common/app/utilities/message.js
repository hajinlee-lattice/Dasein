export const MODAL = 'modal';
export const BANNER = 'banner';
export const NOTIFICATION = 'notification';
export const SUCCESS = 'success';
export const ERROR = 'error';
export const INFO = 'info';
export const WARNING= 'warning';

export default class Message {

    constructor(position, type, message, fullMessage ){
        this.position = position;
        this.type = type;
        this.message = message;
        this.fullMessage = fullMessage;
    }
    getPosition(){
        return this.position;
    }

    getType(){
        return this.type;
    }

    getMessage() {
        return this.message;
    }

    getFullMessage(){
        return this.fullMessage;
    }


}