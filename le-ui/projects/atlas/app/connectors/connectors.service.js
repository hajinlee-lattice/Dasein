export const MARKETO = "Marketo";
export const SALESFORCE = "Salesforce";
export const ELOQUA = "Eloqua";
export const AWS_S3 = "AWS_S3";
export const LINKEDIN = "LinkedIn";
export const FACEBOOK = "Facebook";

import messageService from "common/app/utilities/messaging-service";
import Message, {
	MODAL,
	GENERIC,
	CLOSE_MODAL
} from "common/app/utilities/message";

class ConnectorService {
	constructor() {
		if (!ConnectorService.instance) {
			ConnectorService.instance = this;
			this.userInfo = { validated: false };
			this.connectorInfo = {
				name: ""
			};
			this._connectors = {
				Salesforce: {
					defaultConnector: false,
					name: "Salesforce",
					config: {
						img: "/atlas/assets/images/logo_salesForce_2.png",
						text:
							"Send and receive recommendations about how likely leads, accounts and customers are to buy, what they are likely to buy and when, by connecting to this CRM"
					}
				},
				Marketo: {
					defaultConnector: false,
					name: "Marketo",
					config: {
						img: "/atlas/assets/images/logo_marketo_2.png",
						text:
							"Activate audience segments based on your Customer 360 data to power your email campaigns, by connecting to Marketo"
					}
				},
				Eloqua: {
					defaultConnector: false,
					name: "Eloqua",
					config: {
						img: "/atlas/assets/images/eloqua.png",
						text:
							"Activate audience segments based on your Customer 360 data to power your email campaigns, by connecting to Eloqua"
					}
				},
				AWS_S3: {
					defaultConnector: true,
					name: "AWS_S3",
					config: {
						img: "/atlas/assets/images/amazon-s3.png",
						text: ""
					}
				},
				LinkedIn: {
					defaultConnector: false,
					name: "LinkedIn",
					config: {
						img: "/atlas/assets/images/linkedin.png",
						text:
							"Generate leads and build brand awareness by running social ads on the world’s largest professional network"
					}
				},
				Facebook: {
					defaultConnector: false,
					name: "Facebook",
					config: {
						img: "/atlas/assets/images/facebook.png",
						text:
							"Display social ads to reach your future consumers by marketing to 2 billion people on Facebook"
					}
				}
			};
			this._connectorsList = [];
		}

		return ConnectorService.instance;
	}
	getImgByConnector(connectorName) {
		let element = this._connectors[connectorName];
		// console.log('CN ',connectorName, element);
		if (element) {
			return element.config.img;
		} else {
			return "";
		}
	}
	getConnectorCreationTitle(otherTxt) {
		switch (this.connectorInfo.name) {
			case SALESFORCE:
				return `${"SFDC"} ${otherTxt ? otherTxt : ""}`;
			case ELOQUA:
				return `${"ELOQUA"} ${otherTxt ? otherTxt : ""}`;
			default:
				return otherTxt;
		}
	}

	getConnectorCreationBody() {
		let system = this.getConnectorCreationTitle();
		let h5 = `${"<h5>"}${system} ${"org Authentication</h5>"}`;
		let to = "BIS";
		if (ELOQUA == this.connectorInfo.name) {
			to = "Lattice Predictive Campaigns";
		}
		let p = `${"<p>Generate a One-time Authentication token below to connect the"} ${to} ${"application with Lattice platform</p>"}`;
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

	getList(externalIntegrationEnabled) {
		if (this._connectorsList.length == 0) {
			Object.keys(this._connectors).forEach(element => {
				let el = this._connectors[element];
				if (
					!this.isExternallyAuthenticatedSystem(element) &&
					el &&
					el.defaultConnector == false
				) {
					this._connectorsList.push(this._connectors[element]);
				} else if (
					el &&
					el.defaultConnector == false &&
					externalIntegrationEnabled[el.name] == true
				) {
					this._connectorsList.push(this._connectors[element]);
				}
			});
		}
		return this._connectorsList;
	}
	sendMSG(callbackFn) {
		let title = this.getConnectorCreationTitle("Settings");
		let body = this.getConnectorCreationBody();
		let msg = new Message("Test", MODAL, GENERIC, title, body);
		msg.setConfirmText("Email Token");
		msg.setIcon("fa fa-cog");
		msg.setCallbackFn(args => {
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
	isExternallyAuthenticatedSystem(systemName) {
		switch (systemName) {
			case MARKETO:
			case LINKEDIN:
			case FACEBOOK:
				return true;
			case SALESFORCE:
			case ELOQUA:
			case AWS_S3:
				return false;
		}
	}

	sendMSG(callbackFn) {
		let title = this.getConnectorCreationTitle("Settings");
		let body = this.getConnectorCreationBody();
		let msg = new Message("Test", MODAL, GENERIC, title, body);
		msg.setConfirmText("Email Token");
		msg.setIcon("fa fa-cog");
		msg.setCallbackFn(args => {
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
	isExternallyAuthenticatedSystem(systemName) {
		switch (systemName) {
			case MARKETO:
			case LINKEDIN:
			case FACEBOOK:
				return true;
			case SALESFORCE:
			case ELOQUA:
			case AWS_S3:
				return false;
		}
	}
}

const instance = new ConnectorService();
Object.freeze(instance);

export default instance;
