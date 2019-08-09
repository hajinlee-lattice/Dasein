import React, { Component } from "common/react-vendor";
import { store, injectAsyncReducer } from "store";

import { CENTER, LEFT } from "common/widgets/container/le-alignments";
// import { openConfigWindow, solutionInstanceConfig } from "./configWindow";
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import { actions as modalActions } from "common/widgets/modal/le-modal.redux";
import LeButton from "common/widgets/buttons/le-button";
import ReactRouter from "atlas/react/router";
import "./connections.component.scss";
import LeVPanel from "common/widgets/container/le-v-panel";
import LeHPanel from "common/widgets/container/le-h-panel";
import { LeToolBar, HORIZONTAL } from "common/widgets/toolbar/le-toolbar";
import Connector from "./connector.component";
import { reducer } from "./connections.redux";
// import ConfWindowService from "./confWindowService";
import { LARGE_SIZE } from "common/widgets/modal/le-modal.utils";
import IFrameComponent from "./iframe.component";
import ConnectorService, {
	MARKETO,
	SALESFORCE,
	ELOQUA,
	LINKEDIN,
	FACEBOOK
} from "./connectors.service";

import SystemsListComponent from "./systems-list.component";
import ReactMainContainer from "../react/react-main-container";
import ConfWindowService, { FIELD_MAPPING } from "./confWindowService";

// import { actions as modalActions } from "common/widgets/modal/le-modal.redux";
// import { store } from "store";
export default class ConnectionsComponent extends Component {
	constructor(props) {
		super(props);
		this.ConnectorsService = ReactRouter.getRouter().ngservices.ConnectorsService;
		this.generateAuthTokenClickHandler = this.generateAuthTokenClickHandler.bind(
			this
		);
		this.clickHandler = this.clickHandler.bind(this);
		this.state = {
			connectorSelected: "",
			userValidated: false,
			userInfo: null,
			userName: null,
			accessToken: null,
			authorizationCode: null,
			solutionInstanceId: null,
			openModal: false
		};
		this.connectors = ConnectorService.getList(
			this.ConnectorsService.isExternalIntegrationEnabled()
		);

		this.setIframeMounted = this.setIframeMounted.bind(this);
		this.handleIframeMessages = this.handleIframeMessages.bind(this);
		this.connectors = ConnectorService.getList({
			Marketo: this.ConnectorsService.isExternalIntegrationEnabled(),
			Facebook: this.ConnectorsService.isFacebookIntegrationEnabled(),
			LinkedIn: this.ConnectorsService.isLinkedInIntegrationEnabled()
		});
	}

	handleChange = () => {
		const data = store.getState()["connections"];
		let userName = data.userName;
		let userId = data.userId;
		let accessToken = data.accessToken;
		this.setState({
			userName: userName,
			accessToken: accessToken,
			userValidated: userName != null && userName != undefined
		});
		if (userName && userId && accessToken) {
			this.setState({
				userValidated: true,
				userInfo: {
					userName: userName,
					id: userId,
					accessToken: accessToken
				}
			});
		}
	};

	handleIframeMessages(e) {
		// if (e.origin != "https://embedded.tray.io") {
		// 	return;
		// }

		if (e.data.type === "tray.configPopup.error") {
			// Handle popup error message
			alert(`Error: ${e.data.err}`);
		}
		if (e.data.type === "tray.configPopup.cancel") {
			modalActions.closeModal(store);
		}
		if (e.data.type === "tray.configPopup.finish") {
			// Handle popup finish message
			if (
				ConfWindowService.getSolutionInstanceConfig()
					.registerLookupIdMap == true
			) {
				console.log("Register new system");
				if (ConfWindowService.getSolutionInstanceConfig().id) {
					// get Tray auth values
					// create lookup id map
					// enable solution instance
					ConfWindowService.getTrayAuthValues(
						ConfWindowService.getSolutionInstanceConfig().id
					);
				} else {
					alert("Error: Solution instance id is not defined");
				}
			} else {
				console.log("Update system");
				ConfWindowService.updateSystem();
			}
			// configFinished = true;
			modalActions.closeModal(store);
		}
		if (e.data.type === "tray.configPopup.validate") {
			// Return validation in progress
			if (this.state.iframeRef && this.state.iframeRef.contentWindow) {
				this.state.iframeRef.contentWindow.postMessage(
					{
						type: "tray.configPopup.client.validation",
						data: {
							inProgress: true
						}
					},
					"*"
				);
			}

			setTimeout(() => {
				// Add errors to all inputs
				console.log(e.data.data);
				const errors = e.data.data.visibleValues.reduce(
					(errors, externalId) => {
						console.log(
							`Visible ${externalId} value:`,
							e.data.data.configValues[externalId]
						);
						if (externalId == FIELD_MAPPING) {
							ConfWindowService.verifyFieldMapping(
								e.data.data.configValues[externalId],
								errors,
								externalId
							);
						}
						return errors;
					},
					{}
				);

				// Return validation
				if (
					this.state.iframeRef &&
					this.state.iframeRef.contentWindow
				) {
					this.state.iframeRef.contentWindow.postMessage(
						{
							type: "tray.configPopup.client.validation",
							data: {
								inProgress: false,
								errors: errors
							}
						},
						"*"
					);
				}
			}, 2000);
		}
	}

	setIframeMounted(ref) {
		// this.iframeRef = ref.current;
		this.setState({
			iframeRef: ref.current
		});
		console.log("REFFFFFFF ", this.state);
	}

	clickHandler(name) {
		console.log(name);
		this.setState({
			connectorSelected: name
		});
		ConnectorService.setConnectorName(name);
		let isExternallyAuthenticatedSystem = ConnectorService.isExternallyAuthenticatedSystem(
			name
		);
		if (!isExternallyAuthenticatedSystem) {
			ConnectorService.setUserValidated(true);
		} else if (
			isExternallyAuthenticatedSystem &&
			this.state.userInfo == null
		) {
			ConnectorService.setUserValidated(true);
		}
	}
	generateAuthTokenClickHandler() {
		let connectorName = ConnectorService.getConnectorName();
		let isExternallyAuthenticatedSystem = ConnectorService.isExternallyAuthenticatedSystem(
			connectorName
		);
		if (
			ConnectorService.getConnectorName() != "" &&
			!isExternallyAuthenticatedSystem
		) {
			ConnectorService.sendMSG(() => {
				this.ConnectorsService.generateAuthToken();
			});
		} else if (isExternallyAuthenticatedSystem) {
			this.getSolutionConfiguration(
				this.state.userInfo.id,
				connectorName,
				this.state.userName +
					"_" +
					connectorName +
					"_" +
					new Date().getTime()
			);
		}
	}

	validateUser(userName) {
		console.log(userName);
		let observer = new Observer(
			response => {
				// httpService.printObservables();
				if (response.data && response.data.name) {
					this.setState({
						userValidated: true,
						userInfo: response.data,
						accessToken: response.data.accessToken
					});
					ConfWindowService.getSolutionInstanceConfig().accessToken = this.state.accessToken;
					console.log(
						(!ConnectorService.isUserValidated() &&
							this.state.connectorSelected == "") ||
							(ConnectorService.isExternallyAuthenticatedSystem(
								this.state.connectorSelected
							) &&
								(!this.state.accessToken ||
									!this.state.userInfo))
					);
					httpService.unsubscribeObservable(observer);
				} else {
					this.setState({
						userValidated: false,
						userInfo: {}
					});
				}
			},
			error => {
				console.error("ERROR ", error);
				this.setState({
					userValidated: false
				});
			}
		);

		httpService.get("/tray/user?userName=" + userName, observer);
	}

	getSolutionConfiguration(userId, tag, instanceName) {
		// const configWindow = openConfigWindow();
		ConfWindowService.getSolutionInstanceConfig().orgType = tag;
		let observer = new Observer(
			response => {
				if (response.data && response.data.authorizationCode) {
					var data = response.data;
					this.setState({
						authorizationCode: data.authorizationCode,
						solutionInstanceId: data.solutionInstanceId
					});
					ConfWindowService.getSolutionInstanceConfig().id =
						data.solutionInstanceId;
					ConfWindowService.getSolutionInstanceConfig().registerLookupIdMap = true;
					ConfWindowService.getSolutionInstanceConfig().accessToken = this.state.accessToken;
					// configWindow.location = this.getPopupUrl(data.solutionInstanceId, data.authorizationCode);
					let url = ConnectorService.getPopupUrl(
						data.solutionInstanceId,
						data.authorizationCode
					);
					httpService.unsubscribeObservable(observer);
					let config = {
						callback: action => {
							modalActions.closeModal(store);
							// ConfWindowService.setUpdating(false);
						},
						className: "launch-modal",
						template: () => {
							return (
								<IFrameComponent
									onLoad={() => {
										console.log("Iframe loaded");
									}}
									src={url}
									mounted={this.setIframeMounted}
								/>
							);
						},
						title: () => {},
						titleIcon: () => {
							return (
								<img
									src={ConnectorService.getImgByConnector(
										ConnectorService.getConnectorName()
									)}
								/>
							);
						},
						hideFooter: true,
						size: LARGE_SIZE
					};
					modalActions.openModal(store, config);
				} else {
					console.log("ERROR");
					console.log(response);
				}
			},
			error => {
				console.error("ERROR ", error);
			}
		);
		let userAccessToken = this.state.accessToken;
		httpService.get(
			"/tray/solutionconfiguration?tag=" +
				tag +
				"&userId=" +
				userId +
				"&instanceName=" +
				instanceName,
			observer,
			{ UserAccessToken: userAccessToken }
		);
	}

	getConnectros() {
		let connectors = this.connectors.map((obj, index) => {
			return (
				<Connector
					key={index}
					name={obj.name}
					config={obj.config}
					clickHandler={this.clickHandler}
					setContainer={this.setContainer}
					classNames={`${"le-connector"} ${
						this.state.connectorSelected == obj.name
							? "selected"
							: ""
					}`}
				/>
			);
		});
		return connectors;
	}

	componentDidMount() {
		injectAsyncReducer(store, "connections", reducer);
		window.addEventListener("message", this.handleIframeMessages);
		this.unsubscribe = store.subscribe(this.handleChange);

		this.router = ReactRouter.getRouter();
		if (name != MARKETO) {
			ConnectorService.setUserValidated(true);
		} else {
			this.validateUser(userName);
		}
	}
	componentWillUnmount() {
		window.removeEventListener("message", this.handleIframeMessages);
	}

	render() {
		return (
			<ReactMainContainer>
				<LeVPanel hstrech={"true"}>
					<h2 className="connectors-title">Add a new connection</h2>
					<LeVPanel halignment={CENTER}>
						<LeHPanel
							hstretch={"true"}
							halignment={CENTER}
							valignment={CENTER}
						>
							{/* <button>L</button> */}
							<LeHPanel
								hstretch={"false"}
								halignment={CENTER}
								className="connectors-list"
							>
								{this.getConnectros()}
							</LeHPanel>
							{/* <button>R</button> */}
						</LeHPanel>
					</LeVPanel>
					<LeToolBar direction={HORIZONTAL} className={"strech"}>
						<div className="right">
							<LeButton
								name="credentials"
								disabled={
									(!ConnectorService.isUserValidated() &&
										this.state.connectorSelected == "") ||
									(ConnectorService.isExternallyAuthenticatedSystem(
										this.state.connectorSelected
									) &&
										(!this.state.accessToken ||
											!this.state.userInfo))
								}
								config={{
									label: "Create",
									classNames:
										"gray-button aptrinsic-connections-create-system"
								}}
								callback={this.generateAuthTokenClickHandler}
							/>
						</div>
					</LeToolBar>
					<SystemsListComponent
						iframeMounted={this.setIframeMounted}
					/>
				</LeVPanel>
			</ReactMainContainer>
		);
	}
}
