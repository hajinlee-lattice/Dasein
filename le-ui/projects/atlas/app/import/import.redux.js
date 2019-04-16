
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";

const FETCH_FIELD_MAPPINGS = 'FETCH_FIELD_MAPPINGS';
const FETCH_DATE_SUPPORT = 'FETCH_DATE_SUPPORT';
const SET_INITIAL_MAPPINGS = 'SET_INITIAL_MAPPINGS';
const GET_INITIAL_MAPPINGS = 'GET_INITIAL_MAPPINGS';
const GET_DATE_FORMATS = 'GET_DATE_FORMATS';
const GET_TIME_FORMATS = 'GET_TIME_FORMATS';
const GET_TIMEZONES = 'GET_TIMEZONES';
const initialState = { fieldMappings: [], dateFormats: [], timeFormats: {}, timezones: {} };

export const actions = {
    setInitialMapping: (fieldMappings) => dispatch => {
        dispatch({
            type: SET_INITIAL_MAPPINGS,
            payload: fieldMappings.fieldMappings
        })
    },
    fetch: (fileName, entity, feedType, source) => dispatch => {

        let observer = new Observer(
            response => {
                if (response.status == 200) {
                    return dispatch({
                        type: FETCH_FIELD_MAPPINGS,
                        payload: response.data.Result.fieldMappings
                    });
                }
            },
            error => {
            }
        );
        httpService.post(`${'/pls/models/uploadfile/'}${fileName}${'/fieldmappings?entity=Account&feedType=AccountSchema&source=File'}`, {}, observer);
    },
    getInitialMapping: () => dispatch => {
        return dispatch({
            type: GET_INITIAL_MAPPINGS,
            payload: initialState.fieldMappings

        });
    },
    fetchDateSupport: () => dispatch => {
        let observer = new Observer(
            response => {
                if (response.status == 200) {
                    // console.log('RESPONSE ', response.data.Result);
                    let dateFormats = response.data.Result.date_format;
                    let timeFormats = response.data.Result.time_format;
                    let timezones = response.data.Result.timezones;
                    return dispatch({
                        type: FETCH_DATE_SUPPORT,
                        payload: {
                            dateFormats: dateFormats,
                            timeFormats: timeFormats,
                            timezones: timezones
                        }
                    })
                }
            },
            error => { }
        );
        httpService.get('/pls/models/uploadfile/dateformat', observer, {});
    },
    getDateFormats: () => dispatch => {
        return dispatch({
            type: GET_DATE_FORMATS,
            payload: initialState.dateFormats

        });
    },
    getTimeFormats: () => dispatch => {
        return dispatch({
            type: GET_TIME_FORMATS,
            payload: initialState.timeFormats

        });
    },
    getDateFormats: () => dispatch => {
        return dispatch({
            type: GET_TIMEZONES,
            payload: initialState.timezones

        });
    }

};

export const reducer = (state = initialState, action) => {
    switch (action.type) {
        case FETCH_FIELD_MAPPINGS:
            return {
                ...state,
                fieldMappings: action.payload
            };
        case SET_INITIAL_MAPPINGS:
            return {
                ...state,
                fieldMappings: action.payload
            };
        case GET_INITIAL_MAPPINGS:
            return {
                ...state,
                fieldMappings: action.payload
            };
        case FETCH_DATE_SUPPORT:
            return {
                ...state,
                dateFormats: action.payload.dateFormats,
                timeFormats: action.payload.timeFormats,
                timezones: action.payload.timezones
            };
            case GET_DATE_FORMATS:
            return {
                ...state,
                dateFormats: action.payload
            };
            case GET_TIME_FORMATS:
            return {
                ...state,
                timeFormats: raction.payload
                
            };
            case GET_TIMEZONES:
            return {
                ...state,
                timezones: action.payload
            };
        default:
            return state;
    }
};
