import React, { Component } from "common/react-vendor";
import "./gridtile.component.scss";

export default class LeGridTile extends Component {
	constructor(props) {
		super(props);

		this.state = {
			openCustomMenu: false,
			editing: false,
			attributes: []
		};

		let AttributesStore = props.context.AttributesStore;
		let restrictions = AttributesStore.getTopNAttributes(props.item, 5);
		let cube = props.cube;

		restrictions = AttributesStore.sortAttributesByCnt(restrictions);

		let attrs = AttributesStore.formatAttributes(restrictions, cube)
		//console.log('-!-!- attrs', attrs, restrictions, cube)
	}

	toggle = (path) => {
		var state = Object.assign({}, this.state);
		state[path] = !state[path];
		this.setState(state);
	}

	toggleCustomMenu = () => {
		this.toggle('openCustomMenu');
	}

	toggleEditMode = () => {
		this.toggle('editing');
	}


	formatTS(ts, full) {
		let d = new Date(ts);
		let months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
		let year = d.getFullYear();
		let month = months[d.getMonth()];
		let day = d.getDate();
		let date = month + ' ' + day + ', ' + year;
		return date + (full
			? ' @ ' + d.toLocaleString('en-US', {
				hour: 'numeric',
				minute: 'numeric',
				hour12: true
			})
			: '');
	}

	render() {
		let item = this.props.item || {};

		return (
			<div class="le-gridtile">
				{
					this.state.editing
						? this.renderEditForm(item)
						: [
							this.renderHeader(item),
							this.renderMetadata(item),
							this.renderBody(item),
							this.renderFooter(item)
						]
				}
			</div>
		);
	}

	renderHeader(item) {
		return (
			<div class="tile-header">
				<h2 title={item.display_name}>
					{item.display_name}
				</h2>
				<div class="edit">
					<button type="button"
						onClick={this.toggleCustomMenu}
						className={this.state.openCustomMenu ? 'open' : ''}>
						<span
							className={"fa fa-ellipsis-v" + (this.state.openCustomMenu ? ' fa-rotate-180' : '')}>
						</span>
					</button>
					{this.state.openCustomMenu ? this.renderCustomMenu(item) : ''}
				</div>
			</div>
		);
	}

	renderCustomMenu(item) {
		return (
			<ul class="model-menu">
				<li onClick={this.toggleEditMode}>
					<a href="javascript:void(0)">Edit</a>
					<i class="fa fa-edit"></i>
				</li>
				<li onClick={this.clickDuplicateSegment}>
					<a href="javascript:void(0)">Duplicate</a>
					<i class="fa fa-copy"></i>
				</li>
				<li onClick={this.clickDeleteSegment}>
					<a href="javascript:void(0)">Delete</a>
					<i class="fa fa-trash"></i>
				</li>
			</ul>
		);
	}

	renderMetadata(item) {
		return (
			<div class="tile-metadata" ng-if="!vm.tileStates[item.name].editSegment">
				<ul>
					<li>
						Created by&nbsp;
						<span class="blue-text"
							title={item.created_by}>
							{item.created_by.split('@')[0]}
						</span> on&nbsp;
						<span class="blue-text"
							title={this.formatTS(item.created, true)}>
							{this.formatTS(item.created)}
						</span>
					</li>
					<li>
						Last updated <span class="blue-text">{this.formatTS(item.updated, true)}</span>
					</li>
					<li>
						{item.description
							? (<p title={item.description} className="description">
								Description:&nbsp;
									<span class="blue-text">
									{item.description}{item.description.length > 250 ? '&hellip;' : ''}
								</span>
							</p>)
							: (<p className="description">
								<span
									class="blue-text"
									onClick={this.toggleEditMode}
								>
									Add a description.
									</span>
							</p>)
						}
					</li>
				</ul>
			</div>
		);
	}

	renderBody() {
		return (
			<div class="tile-body wrap-body">
				{
					this.state.invalid
						? <p class="attributesDisplay centered italicized">
							<i class="fa fa-exclamation-triangle" style="color:#ca2b2b"></i>&nsbp;
							This segment contains invalid or deleted attributes.
							</p>
						: this.state.attributes.map(attr => {
							return (<p class="attributesDisplay">
								{attr.label}: <em>{attr.value}</em>
							</p>)
						})
				}
			</div>
		);
	}

	renderFooter(item) {
		return (
			<div class="tile-footer segments">
				<div class="account-count columns six">
					<h4 class="entity-title">ACCOUNTS </h4>
					<div class="blue-text">
						{item.accounts || 0}
					</div>
				</div>
				<div class="contact-count columns six">
					<h4 class="entity-title">CONTACTS </h4>
					<div class="blue-text">
						{item.contacts || 0}
					</div>
				</div>
			</div>
		);
	}

	renderEditForm(item) {
		return (
			<edit-form
				config="vm.editConfig"
				dataobj="segment"
				saving="vm.saveInProgress"
				callback={() => { this.saveNameDescription(obj, newData); }}>
			</edit-form>
		);
	}
}