import React, { Component } from "../../../react-vendor";
import "./sort.scss";

export const DIRECTION_ASC = "asc";
export const DIRECTION_DESC = "desc";
export const DIRECTION_NONE = "none";

let sortingName = "";
let sortingDirection = "";
let sortingType = "string";
let sortCheckGreater = null;
let sortCheckLessThan = null;

const createDataObj = (arrayKeys, value) => {
	if (arrayKeys.length > 1) {
		let newArray = [...arrayKeys];
		let obj = {};
		obj[newArray[newArray.length - 1]] = value;
		newArray.pop();
		return createDataObj(newArray, obj);
	}
	var theKey = arrayKeys[0];
	let ret = {};
	ret[theKey] = value;
	return ret;
};

export const createColumnData = (mapping, fieldName) => {
	let ret = {};
	Object.keys(mapping).forEach(key => {
		let splitKeys = key.split(".");
		let obj = createDataObj(splitKeys, mapping[key][fieldName]);
		if (!ret[Object.keys(obj)[0]]) {
			ret = Object.assign({}, ret, obj);
		} else {
			let keyTmp = Object.keys(obj)[0];
			let subObj = obj[keyTmp];
			let subKey = Object.keys(subObj)[0];
			let subVal = subObj[subKey];
			ret[keyTmp][subKey] = subVal;
		}
	});
	return ret;
};

export const getColumnData = (obj, arrayKeys) => {
	if (arrayKeys.length > 0 && obj) {
		let newObj = obj[arrayKeys[0]];
		arrayKeys.splice(0, 1);
		return getColumnData(newObj, arrayKeys);
	}
	return obj;
};

const compare = (a, b) => {
	let arraySplitA = sortingName.split(".");
	let valA = getColumnData(a, arraySplitA);
	if (!isNaN(valA)) {
		sortingType = "number";
	}
	let arraySplitB = sortingName.split(".");
	let valB = getColumnData(b, arraySplitB);
	switch (sortingType) {
		case "string": {
			sortCheckGreater =
				(valA + "").toLowerCase() < (valB + "").toLowerCase();
			sortCheckLessThan =
				(valA + "").toLowerCase() > (valB + "").toLowerCase();

			switch (sortingDirection) {
				case DIRECTION_ASC: {
					if (sortCheckGreater) {
						return -1;
					}
					if (sortCheckLessThan) {
						return 1;
					}
					return 0;
				}
				case DIRECTION_DESC:
					if (sortCheckLessThan) {
						return -1;
					}
					if (sortCheckGreater) {
						return 1;
					}
					return 0;
			}
		}
		case "number": {
			switch (sortingDirection) {
				case DIRECTION_ASC: {
					return valA - valB;
				}
				case DIRECTION_DESC:
					return valB - valA;
			}
		}
	}
};

export const SortUtil = {
	sortAray: (array, name, direction) => {
		sortingName = name;
		sortingDirection = direction;
		let ret = array.sort(compare);
		sortingType = "string";
		return ret;
	}
};

export default class Sort extends Component {
	constructor(props) {
		super(props);
		this.state = { direction: this.props.direction };
		this.sortHandler = this.sortHandler.bind(this);
	}

	sortHandler() {
		let newDirection = DIRECTION_NONE;
		switch (this.state.direction) {
			case DIRECTION_DESC:
				newDirection = DIRECTION_ASC;
				break;
			case DIRECTION_ASC:
				newDirection = DIRECTION_DESC;
				break;
			default:
				newDirection = DIRECTION_ASC;
		}
		this.setState({ direction: newDirection }, () => {
			this.props.callback(this.props.colName, this.state.direction);
		});
	}

	getDirection() {
		switch (this.props.direction) {
			case DIRECTION_DESC:
				return DIRECTION_DESC;
			case DIRECTION_ASC:
				return DIRECTION_ASC;
			default:
				return DIRECTION_NONE;
		}
	}
	getSortIcon() {
		let direction = this.getDirection();
		let className = "fa fa-sort";
		if (direction != DIRECTION_NONE) {
			return `${className}-${direction}`;
		} else {
			return className;
		}
	}

	render() {
		return (
			<div className="sort-container" onClick={this.sortHandler}>
				<i className={this.getSortIcon()} aria-hidden="true" />
			</div>
		);
	}
}
