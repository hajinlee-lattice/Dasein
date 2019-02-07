export const MARKETO = 'Marketo';
export const SALESFORCE = 'Salesforce';
export const ELOQUA = 'Eloqua';

import messageService from 'common/app/utilities/messaging-service';
import Message, {
    MODAL,
    GENERIC,
    CLOSE_MODAL
} from 'common/app/utilities/message';
class ConnectorService {
    constructor() {
        if (!ConnectorService.instance) {
            console.log('============>Creating instance');
            ConnectorService.instance = this;
            this.userInfo = { validated: false };
            this.connectorInfo = {
                name: ''
            };
            this._connectorsList = [
                {
                    name: 'Salesforce',
                    config: { img: '/atlas/assets/images/logo_salesForce_2.png', text: 'Send and receive reccomandations about how likely leads, accounts and customers are to buy, what they are likely to buy and when, by connecting to this CRM' },
                },
                {
                    name: 'Marketo',
                    config: { img: '/atlas/assets/images/logo_marketo_2.png', text: 'Activate audience segments based on your Customer 360 data to power your email campaigns, by connecting to Marketo' }
                },
                {
                    name: 'Eloqua',
                    config: { img: '/atlas/assets/images/eloqua.png', text: 'Activate audience segments based on your Customer 360 data to power your email campaigns, by connecting to Eloqua' }
                }
            ];
        }

        return ConnectorService.instance;
    }
    getImgByConnector(connectorName){
        let path = ''
        this._connectorsList.forEach(element => {
            if(element.name === connectorName){
                
                path = element.config.img;
                return;
            }
        });
        console.log('IMAGE PATH ', path);
        return path;
    }
    setConnectorName(name) {
        this.connectorInfo.name = name;
    }
    getConnectorName() {
        return this.connectorInfo.name;
    }
    setUserValidated(validated) {
        this.userInfo.validated = validated;
    }
    isUserValidated() {
        return this.userInfo.validated;
    }

    setList(list) {
        this._connectorsList = list;
    }

    getList() {
        console.log('Getting List <=====================');
        return this._connectorsList;
    }
    sendMSG(title, message, callbackFn) {
        let msg = new Message(
            'Test',
            MODAL,
            GENERIC,
            'SFDC Settings',
            `<h5>SFDC org Authentiocation</h5>
            <p>Generate a One-time Authentication token below to connect the BIS application with Lattice platform</p>
            `
        );
        msg.setConfirmText('Email Token');
        msg.setCallbackFn((args) => {
            console.log('Callback', args);
            if (args.action !== "closedForced") {
                let closeMsg = new Message([], CLOSE_MODAL);
                closeMsg.setName(args.name);
                closeMsg.setCallbackFn();
                messageService.sendMessage(closeMsg);
                callbackFn();
            }
        });
        messageService.sendMessage(msg);
    }
}

const instance = new ConnectorService();
Object.freeze(instance);

export default instance;

