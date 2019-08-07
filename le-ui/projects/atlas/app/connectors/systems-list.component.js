import React, { Component } from "common/react-vendor";
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import ReactRouter from "../react/router";

import GridLayout from "common/widgets/container/grid-layout.component";

import SystemComponent from "./system.component";

import SystemsService from "./systems.service";
import ConnectorService from "./connectors.service";
import LeVPanel from "common/widgets/container/le-v-panel";
import "./systems-list.component.scss";
import ConfWindowService, { FIELD_MAPPING } from "./confWindowService";
import { MEDIUM_GAP } from "../../../common/widgets/container/grid-layout.component";
import { actions as modalActions } from "common/widgets/modal/le-modal.redux";
import { store } from "store";

export default class SystemsListComponent extends Component {
	constructor(props) {
		super(props);
		this.state = {
			connectors: [],
			loading: true,
			generatedAuthCode: false,
			iframeRef: undefined
		};
		this.getConnectors = this.getConnectors.bind(this);
		let FeatureFlagService = ReactRouter.getRouter().ngservices
			.FeatureFlagService;
		this.alfaFeature = FeatureFlagService.FlagIsEnabled(
			FeatureFlagService.Flags().ALPHA_FEATURE
		);
		this.setContainer = this.setContainer.bind(this);
		this.handleIframeMessages = this.handleIframeMessages.bind(this);
	}

	getConnectors(response) {
		let connectors = [];
		let CRMs = response.data.CRM || [];
		let MAPs = response.data.MAP || [];
		let FILE_SYSTEM = response.data.FILE_SYSTEM || [];
		if (this.alfaFeature) {
			connectors = FILE_SYSTEM.concat(CRMs, MAPs);
		} else {
			connectors = CRMs.concat(MAPs);
		}
		return connectors;
	}

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

	setContainer(ref) {
		// this.iframeRef = ref.current;
		this.setState({
			iframeRef: ref.current
		});
		console.log("REFFFFFFF ", this.state);
	}

	getSystems() {
		let ret = [];
		if (this.state.connectors && this.state.connectors.length > 0) {
			this.state.connectors.forEach(system => {
				// console.log(system.externalSystemName);
				ret.push(
					<SystemComponent
						system={system}
						setContainer={this.setContainer}
						config={{
							img: ConnectorService.getImgByConnector(
								system.externalSystemName
							)
						}}
					/>
				);
			});
		} else {
			switch (this.state.loading) {
				case true:
					ret.push(<div />);
					ret.push(
						<div
							style={{
								display: "flex",
								justifyContent: "center"
							}}
						>
							<i className="fa fa-spinner fa-spin fa-2x fa-fw" />
						</div>
					);
					break;
			}
		}

		return ret;
	}

	getCount() {
		if (!this.state.loading) {
			return (
				<h2 className="systems-header">
					<strong className="systems-count">
						{this.state.count}
					</strong>
					<span>connections</span>
				</h2>
			);
		} else {
			return null;
		}
	}

	componentDidMount() {
		this.setState({ loading: true });
		window.addEventListener("message", this.handleIframeMessages);
		httpService.get(
			"/pls/lookup-id-mapping",
			new Observer(response => {
				let connectors = this.getConnectors(response);
				SystemsService.cleanupLookupId(connectors);
				this.setState({
					loading: false,
					connectors: connectors,
					count: connectors.length
				});
			})
		);
	}

	componentWillUnmount() {
		window.removeEventListener("message", this.handleIframeMessages);
	}

	render() {
		return (
			<LeVPanel hstretch={"true"} className="systems-main">
				{this.getCount()}
				<GridLayout gap={MEDIUM_GAP} classNames="systems-list extends">
					{this.getSystems()}
				</GridLayout>
			</LeVPanel>
		);
	}
}
