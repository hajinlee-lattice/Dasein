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
            ConnectorService.instance = this;
            this.userInfo = { validated: false };
            this.connectorInfo = {
                name: ''
            };
            this._connectors = {
                Salesforce: {
                    name: 'Salesforce',
                    config: { img: '/atlas/assets/images/logo_salesForce_2.png', text: 'Send and receive reccomandations about how likely leads, accounts and customers are to buy, what they are likely to buy and when, by connecting to this CRM' }
                },
                Marketo: {
                    name: 'Marketo',
                    config: { img: '/atlas/assets/images/logo_marketo_2.png', text: 'Activate audience segments based on your Customer 360 data to power your email campaigns, by connecting to Marketo' }
                },
                Eloqua: {
                    name: 'Eloqua',
                    config: { img: '/atlas/assets/images/eloqua.png', text: 'Activate audience segments based on your Customer 360 data to power your email campaigns, by connecting to Eloqua' }
                }
            };
            this._connectorsList = [

            ];
        }

        return ConnectorService.instance;
    }
    getImgByConnector(connectorName) {
        let element = this._connectors[connectorName];
        return element.config.img;
        // let path = ''
        // this._connectorsList.forEach(element => {
        //     if (element.name === connectorName) {

        //         path = element.config.img;
        //         return;
        //     }
        // });
        // return path;
    }
    getConnectorCreationTitle(otherTxt) {
        switch (this.connectorInfo.name) {
            case SALESFORCE:
                return `${'SFDC'} ${otherTxt ? otherTxt : ''}`;
            case ELOQUA:
                return `${'ELOQUA'} ${otherTxt ? otherTxt : ''}`;
            default:
                return otherTxt;
        }
    }

    getConnectorCreationBody() {
        let system = this.getConnectorCreationTitle();
        let h5 = `${'<h5>'}${system} ${'org Authentication</h5>'}`;
        let p = '<p>Generate a One-time Authentication token below to connect the BIS application with Lattice platform</p>';
        return `${h5}${p}`;
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

    getList(markettoEnabled) {
        if (this._connectorsList.length == 0) {
            Object.keys(this._connectors).forEach(element => {
                let el = this._connectors[element];
                if (element !== MARKETO && el) {
                    this._connectorsList.push(this._connectors[element]);
                } else if (markettoEnabled && el) {
                    this._connectorsList.push(this._connectors[element]);
                }
            });
        }
        return this._connectorsList;
    }
    sendMSG(callbackFn) {
        let title = this.getConnectorCreationTitle('Settings');
        let body = this.getConnectorCreationBody();
        let msg = new Message(
            'Test',
            MODAL,
            GENERIC,
            title,
            body
        );
        msg.setConfirmText('Email Token');
        msg.setIcon('fa fa-cog');
        msg.setCallbackFn((args) => {
            let closeMsg = new Message([], CLOSE_MODAL);
            closeMsg.setName(args.name);
            closeMsg.setCallbackFn();
            messageService.sendMessage(closeMsg);
            if (args.action == "ok") {
                callbackFn();
            }
        });
        messageService.sendMessage(msg);
    }
}

const instance = new ConnectorService();
Object.freeze(instance);

export default instance;

