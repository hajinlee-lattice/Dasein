import React, { Component } from "common/react-vendor";
import LeTile from "widgets/container/tile/le-tile";
import LeTileList from "widgets/container/tile/le-tile-list";
//import LeTable from "widgets/table/le-table";

export default class LeSegmentItem extends Component {
	constructor(props) {
		super(props);

		this.state = {
			openCustomMenu: false,
			editing: false,
			attributes: []
		};
	}

	getEnrichmentsMap() {
		let AttributesStore = this.props.context.AttributesStore;
		let restrictions = AttributesStore.getTopNAttributes(this.props.item, 5);
		let enrichments = this.props.enrichments;
		let cube = this.props.cube;

		restrictions = AttributesStore.sortAttributesByCnt(restrictions);

		let attrs = AttributesStore.formatAttributes(restrictions, cube, enrichments) || [];

		this.state.attributes = attrs;
		//console.log('<segment_item>', '\ncube:', cube, '\nenrichments:', enrichments, '\nrestrictions:', restrictions, '\nattrs', attrs);
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
		if (this.state.attributes.length == 0) {
			this.getEnrichmentsMap();
		}

		let item = this.props.item || {};
		let editform = this.renderEditForm(item);
		let content = [
			this.renderHeader(item),
			this.renderMetadata(item),
			this.renderBody(item),
			this.renderFooter(item)
		];

		switch (this.props.view) {
			case 'grid': return this.renderTile(LeTile, content, editform);
			case 'list': return this.renderTile(LeTileList, content, editform);
			case 'table': return this.renderTable(LeTileList, content, editform);
		}
	}

	renderTile(TileComponent, content, editform) {
		return (
			<TileComponent>
				{this.state.editing ? editform : content}
			</TileComponent>
		);
	}

	renderHeader(item) {
		return (
			<div class="tile-header">
				<h2 title={item.display_name}>
					{item.display_name}
				</h2>
				{
					console.log('renderHeader', this.props.view, this.state.openCustomMenu, item, this)
				}{
					this.props.view == 'grid' ? this.renderCustomMenu(item) : ''
				}
			</div>
		);
	}

	renderCustomMenu(item) {
		console.log('renderCustomMenu', item, this.props.view)
		return (
			<div class="edit">
				<button type="button"
					onClick={this.toggleCustomMenu}
					className={this.state.openCustomMenu ? 'open' : ''}>
					<span
						className={"fa fa-ellipsis-v" + (this.state.openCustomMenu ? ' fa-rotate-180' : '')}>
					</span>
				</button>
				{() => {
					return (this.state.openCustomMenu ? (<ul class="model-menu">
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
					</ul>) : '')
				}}
			</div>
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
								{attr.label} <em>{attr.value}</em>
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
				{this.props.view == 'list' ? this.renderCustomMenu(item) : ''}
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