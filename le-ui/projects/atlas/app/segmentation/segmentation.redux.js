import httpService from "app/http/http-service";
import Observer from "app/http/observer";
import { store } from 'store';

var CONST = {
    SET_CONTEXT: 'SET_CONTEXT',
    SET_ITEMBAR: 'SET_ITEMBAR',
    SET_ITEMVIEW: 'SET_ITEMVIEW',
    GET_ENRICHMENTS: 'GET_ENRICHMENTS',
    GET_SEGMENTS: 'GET_SEGMENTS',
    CLEAR_SEGMENTS: 'CLEAR_SEGMENTS',
    GET_CUBE: 'GET_CUBE',
    CLEAR_CUBE: 'CLEAR_CUBE'
}

const initialState = {
    context: {},
    itembar: [],
    itemview: {},
    segments: [],
    enrichments: [],
    filters: {},
    cube: {}
};

export const actions = {
    setContext: (payload) => {
        return store.dispatch({
            type: CONST.SET_CONTEXT,
            payload: payload
        });
    },
    setItemBar: (payload) => {
        return store.dispatch({
            type: CONST.SET_ITEMBAR,
            payload: payload
        });
    },
    setItemView: (payload) => {
        return store.dispatch({
            type: CONST.SET_ITEMVIEW,
            payload: payload
        });
    },
    getEnrichments: (enrichments) => {
        console.log('getEnrichments', enrichments, store.dispatch);
        if (enrichments.length > 0) {
            return store.dispatch({
                type: CONST.GET_ENRICHMENTS,
                payload: enrichments
            });
        }

        httpService.get(
            "/pls/datacollection/attributes",
            new Observer(response => {
                store.dispatch({
                    type: CONST.GET_ENRICHMENTS,
                    payload: response.data
                });
            }), {
                ErrorDisplayMethod: "Banner",
                ErrorDisplayOptions: '{"title": "Warning"}',
                ErrorDisplayCallback: "TemplatesStore.checkIfRegenerate"
            }
        );
    },
    getSegments: () => {
        httpService.get(
            "/pls/datacollection/segments",
            new Observer(response => {
                store.dispatch({
                    type: CONST.GET_SEGMENTS,
                    payload: response.data
                });
            }), {
                ErrorDisplayMethod: "Banner",
                ErrorDisplayOptions: '{"title": "Warning"}',
                ErrorDisplayCallback: "TemplatesStore.checkIfRegenerate"
            }
        );
    },
    getCube: (cube) => {
        if (cube && cube.data) {
            return store.dispatch({
                type: CONST.GET_CUBE,
                payload: cube.data
            });
        }

        httpService.get(
            "/pls/datacollection/statistics/cubes",
            new Observer(response => {
                store.dispatch({
                    type: CONST.GET_CUBE,
                    payload: response.data
                });
            }), {
                ErrorDisplayMethod: "Banner",
                ErrorDisplayOptions: '{"title": "Warning"}',
                ErrorDisplayCallback: "TemplatesStore.checkIfRegenerate"
            }
        );
    },
    clearSegments: () => {
        return store.dispatch({
            type: CONST.CLEAR_SEGMENTS
        });
    },
    clearCube: () => {
        return store.dispatch({
            type: CONST.CLEAR_CUBE
        });
    }
};

export const reducer = (state = initialState, action) => {
    switch (action.type) {
        case CONST.SET_CONTEXT:
            return {
                ...state,
                context: action.payload
            };
        case CONST.SET_ITEMBAR:
            return {
                ...state,
                itembar: action.payload
            };
        case CONST.SET_ITEMVIEW:
            return {
                ...state,
                itemview: action.payload
            };
        case CONST.GET_ENRICHMENTS:
            return {
                ...state,
                enrichments: action.payload
            };
        case CONST.GET_SEGMENTS:
            return {
                ...state,
                segments: action.payload
            };
        case CONST.CLEAR_SEGMENTS:
            return {
                ...state,
                segments: []
            };
        case CONST.GET_CUBE:
            return {
                ...state,
                cube: action.payload
            };
        case CONST.CLEAR_CUBE:
            return {
                ...state,
                cube: {}
            };
        default:
            return state;
    }
};
