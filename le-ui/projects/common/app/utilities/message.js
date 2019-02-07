export const MODAL = 'modal';
export const BANNER = 'banner';
export const NOTIFICATION = 'notification';
export const SUCCESS = 'success';
export const ERROR = 'error';
export const INFO = 'info';
export const WARNING= 'warning';
export const GENERIC = 'generic';
export const CLOSE_MODAL = 'close_modal';

export default class Message {

    constructor(response, position, type, message, fullMessage){
        this.response = response;
        this.position = position;
        this.type = type;
        this.message = message;
        this.fullMessage = fullMessage;
        this.errorUtility = true;
        if(!response || !response.data){
            this.errorUtility = false;
        }

    }
    setConfirmText(confirmText){
        this.confirmText = confirmText;
    }

    getConfirmText(){
        return this.confirmText;
    }

    setIcon(icon){
        this.icon = icon;
    }
    getIcon(){
        return this.icon;
    }

    setCallbackFn(callbackFn){
        this.callback = callbackFn;
    }
    getCallbackFn(){
        return this.callback;
    }
    setName(name){
        this.name = name;
    }
    getName(){
        return this.name;
    }
    getResponse() {
        return this.response;
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

    isErrorUtility(){
        return this.errorUtility;
    }


}