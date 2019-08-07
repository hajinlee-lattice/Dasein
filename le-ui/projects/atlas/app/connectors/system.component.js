import React, { Component } from "common/react-vendor";

import "./system.component.scss";
import Aux from "common/widgets/hoc/_Aux";
import LeTile from "common/widgets/container/tile/le-tile";
import LeTileHeader from "common/widgets/container/tile/le-tile-header";
import LeTileBody from "common/widgets/container/tile/le-tile-body";
import LeTileFooter from "common/widgets/container/tile/le-tile-footer";
import LeButton from "common/widgets/buttons/le-button";
import SystemMappingComponent from "./system-mapping.component";
// import { openConfigWindow, solutionInstanceConfig } from "./configWindow";
import ConfWindowService, { FIELD_MAPPING } from "./confWindowService";
import ConnectorService, {
	MARKETO,
	SALESFORCE,
	ELOQUA
} from "./connectors.service";
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";

import "./systems-list.component.scss";
import LeHPanel from "common/widgets/container/le-h-panel";
import GridLayout from "common/widgets/container/grid-layout.component";
import { RIGHT, CENTER } from "common/widgets/container/le-alignments";
import { actions, reducer } from "./connections.redux";

import { actions as modalActions } from "common/widgets/modal/le-modal.redux";
import { store, injectAsyncReducer } from "store";

import SystemService from "./system.service";
import ReactRouter from "atlas/react/router";
import { LARGE_SIZE } from "common/widgets/modal/le-modal.utils";
import IFrameComponent from "./iframe.component";

export default class SystemComponent extends Component {
	constructor(props) {
		super(props);
		this.state = {
			system: props.system,
			openModal: false,
			saving: false,
			userId: null,
			userAccessToken: null
		};
		this.editMappingClickHandler = this.editMappingClickHandler.bind(this);
		this.modalCallback = this.modalCallback.bind(this);
		this.getEditTemplate = this.getEditTemplate.bind(this);
		this.editMapping = Object.assign({}, props.system);
		this.mappingClosed = this.mappingClosed.bind(this);
		this.TemplatesStore = ReactRouter.getRouter().ngservices.TemplatesStore;
	}

	handleChange = () => {
		const data = store.getState()["connections"];
		let userId = data.userId;
		let accessToken = data.accessToken;
		this.setState({
			userId: userId,
			userAccessToken: accessToken
		});
	};

	componentDidMount() {
		injectAsyncReducer(store, "connections", reducer);
		this.unsubscribe = store.subscribe(this.handleChange);
	}

	editMappingClickHandler() {
		if (!this.isExternallyAuthenticatedSystem()) {
			let config = {
				callback: this.modalCallback,
				template: () => {
					this.editMapping = Object.assign({}, this.state.system);
					return (
						<SystemMappingComponent
							system={this.editMapping}
							closed={this.mappingClosed}
						/>
					);
				},
				title: () => {
					return <p>Org ID to Account ID Mapping</p>;
				}
			};
			modalActions.openModal(store, config);
		} else if (this.state.userId && this.state.userAccessToken) {
			// const configWindow = openConfigWindow();
			var solutionInstanceId = this.state.system.externalAuthentication
				.solutionInstanceId;

			let observer = new Observer(
				response => {
					if (response.data) {
						var authorizationCode = response.data.code;
						ConfWindowService.getSolutionInstanceConfig().id = solutionInstanceId;
						ConfWindowService.getSolutionInstanceConfig().orgType = this.state.system.externalSystemName;
						ConfWindowService.getSolutionInstanceConfig().accessToken = this.state.accessToken;
						ConfWindowService.getSolutionInstanceConfig().registerLookupIdMap = false;
						ConfWindowService.getSolutionInstanceConfig().system = this.state.system;
						let url = this.getPopupUrl(
							solutionInstanceId,
							authorizationCode
						);
						httpService.unsubscribeObservable(observer);

						let config = {
							callback: action => {
								modalActions.closeModal(store);
								ConfWindowService.setUpdating(false);
							},
							className: "launch-modal",
							template: () => {
								return (
									<IFrameComponent
										onLoad={() => {
											console.log("Iframe loaded");
										}}
										src={url}
										mounted={this.props.setContainer}
									/>
								);
							},
							title: () => {
								return <p />;
							},

							hideFooter: true,
							size: LARGE_SIZE
						};
						modalActions.openModal(store, config);
					}
				},
				error => {
					console.error("No authorization code generated");
				}
			);

			httpService.get(
				"/tray/authorizationcode?userId=" + this.state.userId,
				observer
			);
		}
	}

	getPopupAuthorizationCode(solutionInstanceId) {
		console.log(ConnectorService.getTrayUserName());
		let observer = new Observer(
			response => {
				if (response.data) {
					this.setState({
						accessToken: response.data.token
					});

					httpService.unsubscribeObservable(observer);
				}
			},
			error => {
				console.error("No authorization code generated");
			}
		);

		httpService.get("/tray/authorizationcode?userId=" + userId, observer);
	}

	getPopupUrl(solutionInstanceId, authorizationCode) {
		let partnerId = "LatticeEngines";
		return `https://app.tray.io/external/solutions/${partnerId}/configure/${solutionInstanceId}?code=${authorizationCode}&show=[2]&start=2&customValidation=true`;
	}

	mappingClosed(system) {
		if (this.state.saving) {
			let observer = new Observer(response => {
				// httpService.printObservables();
				// console.log('HEY ', response);
				if (response.data) {
					let tmp = response.data;
					this.setState({
						saving: false,
						system: tmp
					});
					httpService.unsubscribeObservable(observer);
				}
			});

			httpService.put(
				"/pls/lookup-id-mapping/config/" + this.state.system.configId,
				system,
				observer
			);
		}
	}

	modalCallback(action) {
		switch (action) {
			case "close":
				modalActions.closeModal(store);
				ConfWindowService.setUpdating(false);
				break;
			case "ok":
				this.setState({ saving: true });
				modalActions.closeModal(store);
				ConfWindowService.setUpdating(false);
				break;
		}
	}

	getSystemStatusClass() {
		let color = "color-";
		switch (this.state.system.isRegistered) {
			case true:
				color = `${color}${"green"}`;
				break;
			default:
				color = `${color}${"red"}`;
				break;
		}
		return color;
	}

	isExternallyAuthenticatedSystem() {
		return this.state.system.externalAuthentication != null;
	}

	getEditTemplate() {
		console.log("TEMPLATE ", this.editMapping);
		this.editMapping = Object.assign({}, this.state.system);
		return (
			<SystemMappingComponent
				system={this.editMapping}
				closed={this.mappingClosed}
			/>
		);
	}

	getAccountIdRow() {
		if (SystemService.canHaveAccountId(this.state.system)) {
			return (
				<Aux>
					<span className="s-label">Account Id:</span>
					<span
						className="s-text"
						title={this.state.system.accountId}
					>
						{this.state.system.accountId}
					</span>
				</Aux>
			);
		}
		return null;
	}

	getNewTokenButton() {
		return (
			<LeButton
				disabled={this.state.saving || !this.state.system.isRegistered}
				config={{
					label: "New Token",
					classNames: "blue-button"
				}}
				callback={() => {
					this.TemplatesStore.newToken();
				}}
			/>
		);
	}
	getActionsButtons() {
		switch (this.state.system.externalSystemType) {
			case "FILE_SYSTEM":
				return (
					<div className="files-system-actions">
						{this.getNewTokenButton()}
						<LeButton
							disabled={
								this.state.saving ||
								!this.state.system.isRegistered
							}
							config={{
								label: "Get Existing Token",
								classNames: "blue-button"
							}}
							callback={() => {
								this.TemplatesStore.getExistingToken();
							}}
						/>
					</div>
				);
			default:
				return (
					<LeButton
						name={`${"edit-mappings-"}${this.state.system.orgName}`}
						disabled={
							this.state.saving ||
							!this.state.system.isRegistered ||
							!SystemService.canEditMapping(this.state.system)
						}
						config={{
							label: "Edit Mappings",
							classNames:
								"blue-button aptrinsic-connections-edit-mappings"
						}}
						callback={this.editMappingClickHandler}
					/>
				);
		}
	}

	getConnectionImgClass() {
		// awss3
		switch (this.state.system.externalSystemType) {
			case "FILE_SYSTEM":
				return `${"s-image"} ${"awss3"}`;
			default:
				return `${"s-image"}`;
		}
	}

	render() {
		return (
			<Aux>
				<LeTile classNames={"system-tile"}>
					<LeTileHeader classNames={"system-header"}>
						<LeHPanel valignment={CENTER}>
							<img
								src={this.props.config.img}
								className={this.getConnectionImgClass()}
							/>
							<p className="s-title">
								{this.state.system.externalSystemName}
							</p>
						</LeHPanel>
					</LeTileHeader>
					<LeTileBody classNames={"s-body"}>
						<GridLayout classNames="system-body-container">
							<span className="s-label">System Org Name:</span>
							<span className="s-text">
								{this.state.system.orgName}
							</span>
							<span className="s-label">System Org Id:</span>
							<span className="s-text">
								{this.state.system.orgId}
							</span>
							{this.getAccountIdRow()}
							<span className="s-label">Last Updated:</span>
							<span className="s-text">
								{SystemService.getValueDateFormatted(
									this.state.system.updated
								)}
							</span>
							<span className="s-label">Status:</span>
							<span
								className={`${"s-text"} ${this.getSystemStatusClass()}`}
							>
								{SystemService.getSystemStatus(
									this.state.system
								)}
							</span>
						</GridLayout>
					</LeTileBody>
					<LeTileFooter classNames={"system-footer"}>
						<LeHPanel
							hstretch={true}
							halignment={RIGHT}
							className="s-controls"
						>
							{this.getActionsButtons()}
						</LeHPanel>
					</LeTileFooter>
				</LeTile>
			</Aux>
		);
	}
}
