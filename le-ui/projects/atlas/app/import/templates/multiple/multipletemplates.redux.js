import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import { store } from "store";

var CONST = {
	FETCH_TEMPLATES: "FETCH_TEMPLATES",
	RESET_TEMPLATES: "RESET_TEMPLATES",
	FETCH_PRIORITIES: "FETCH_PRIORITIES",
	FETCH_SYSTEMS: "FETCH_SYSTEMS",
	RESET_PRIORITIES: "RESET_PRIORITIES",
	SAVE_PRIORITIES: "SAVE_PRIORITIES",
	SYSTEM_SELECTED: "SYSTEM_SELECTED"
};
const initialState = {
	templates: { loaded: false, data: [] },
	priorities: [],
	systemSelected: {}
};

export const actions = {
	setSystemSelected: system => {
		return store.dispatch({
			type: CONST.SYSTEM_SELECTED,
			payload: { systemSelected: system }
		});
	},

	resetTemplates: () => {
		return store.dispatch({
			type: CONST.RESET_TEMPLATES,
			payload: { data: [], loaded: false }
		});
	},
	fetchTemplates: () => {
		let observer = new Observer(response => {
			httpService.unsubscribeObservable(observer);
			return store.dispatch({
				type: CONST.FETCH_TEMPLATES,
				payload: { data: response.data, loaded: true }
			});
		});
		httpService.get(
			"/pls/cdl/s3import/template?sortBy=SystemDisplay",
			observer,
			{}
		);
	},
	fetchPriorities: () => {
		let observer = new Observer(response => {
			httpService.unsubscribeObservable(observer);
			return store.dispatch({
				type: CONST.FETCH_PRIORITIES,
				payload: response.data
			});
		});
		httpService.get("/pls/cdl/s3import/system/list", observer, {});
	},
	resetPriorities: () => {
		return store.dispatch({
			type: CONST.RESET_PRIORITIES,
			payload: []
		});
	},
	savePriorities: newList => {
		let observer = new Observer(response => {
			httpService.unsubscribeObservable(observer);
			return store.dispatch({
				type: CONST.SAVE_PRIORITIES,
				payload: { saved: true }
			});
		});
		httpService.post(
			"../pls/cdl/s3import/system/list",
			newList,
			observer,
			{}
		);
	},
	fetchSystems: (optionsObj, selectedSystem) => {
		// console.log("OOOOOOO ===> ", selectedSystem);
		let observer = new Observer(response => {
			let data = response.data;
			// console.log("FIRST ==> ", data);
			if (selectedSystem) {
				data = data.filter(el => {
					// console.log("EL ==> ", el);
					let ok = el.system_type != selectedSystem.system_type;
					return ok;
				});
			}
			// console.log("DATA ------> ", data);
			httpService.unsubscribeObservable(observer);
			return store.dispatch({
				type: CONST.FETCH_SYSTEMS,
				payload: data
			});
		});
		let options = "";
		Object.keys(optionsObj).forEach((option, index) => {
			options = options.concat(`${option}${"="}${optionsObj[option]}`);
			if (index < Object.keys.length - 1) {
				options = options.concat("&");
			}
		});
		let url = `${"../pls/cdl/s3import/system/list?"}${options}`;
		httpService.get(url, observer, {});
	}
};

export const reducer = (state = initialState, action) => {
	switch (action.type) {
		case CONST.SYSTEM_SELECTED:
			return {
				...state,
				systemSelected: action.payload.systemSelected
			};
		case CONST.FETCH_TEMPLATES:
			return {
				...state,
				templates: {
					data: action.payload.data,
					loaded: action.payload.loaded
						? action.payload.loaded
						: false
				}
			};
		case CONST.RESET_TEMPLATES:
			return {
				...state,
				templates: {
					data: action.payload.data,
					loaded: action.payload.loaded
						? action.payload.loaded
						: false
				}
			};
		case CONST.FETCH_PRIORITIES:
			return {
				...state,
				priorities: action.payload
			};
		case CONST.RESET_PRIORITIES:
			return {
				...state,
				priorities: action.payload
			};
		case CONST.SAVE_PRIORITIES:
			return {
				...state,
				saved: action.payload.saved
			};
		case CONST.FETCH_SYSTEMS:
			return {
				...state,
				systems: action.payload
			};
		default:
			return state;
	}
};
