import React, { Component } from "common/react-vendor";
import NgState from "atlas/ng-state";
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
				{content}
			</TileComponent>
		);
	}

	tileClick(play) {
		NgState.getAngularState().go('home.playbook.overview', {play_name: play.name});
	};

	renderHeader(item) {
		return (
			<div class="tile-header">
				<h2 title={item.displayName} onClick={() => {
					this.tileClick(item);
				}}>
					{item.displayName}
				</h2>
				{this.props.view == 'grid' ? this.renderCustomMenu(item) : ''}
			</div>
		);
	}

	renderCustomMenu(item) {
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
	                	Last Edited by: 
	                	<span class="blue-text">
	                		{item.updatedBy || '--'}
	                	</span>
	               	</li>
	                <li>
	                	Last Edited: 
	                	<span class="blue-text">
	                		{new Date(item.updated).toLocaleDateString("en-US")}
	                	</span>
	                </li>
	                <li>
	                	Segment: 
	                	<span class="blue-text">
	                		{(item.targetSegment && item.targetSegment.display_name ? item.targetSegment.display_name : '--')}
	                	</span>
	                </li>
	                <li>
	                	Model: 
	                	<span class="blue-text">
	                		{(item.ratingEngine && item.ratingEngine.displayName ?  item.ratingEngine.displayName :  '--')}
	                	</span>
	                </li>
				</ul>
			</div>
		);
	}

	renderBody() {
		return (
			<div class="tile-body wrap-body">
				channels will go here
			</div>
		);
	}

	renderFooter(item) {
		return (
			<div class="tile-footer segments">
				<div class="account-count columns six">
					<h4 class="entity-title">Available Accounts</h4>
					<div class="blue-text">
						{item.targetSegment.accounts.toLocaleString() || 0}
					</div>
				</div>
				<div class="contact-count columns six">
					<h4 class="entity-title">Available Contacts</h4>
					<div class="blue-text">
                        {item.targetSegment.contacts.toLocaleString() || 0}
					</div>
				</div>
				{this.props.view == 'list' ? this.renderCustomMenu(item) : ''}
			</div>
		);
	}
}