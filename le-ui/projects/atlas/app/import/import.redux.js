
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";

const FETCH_FIELD_MAPPINGS = 'FETCH_FIELD_MAPPINGS';
const SET_INITIAL_MAPPINGS = 'SET_INITIAL_MAPPINGS';
const GET_INITIAL_MAPPINGS = 'GET_INITIAL_MAPPINGS';
const initialState = { fieldMappings: [] };

export const actions = {
    setInitialMapping: (fieldMappings) => dispatch => {
        // console.log('SET INITIAL MAPPING');
        dispatch({
            type: SET_INITIAL_MAPPINGS,
            payload: fieldMappings.fieldMappings
        })
    },
    fetch: (fileName, entity, feedType, source) => dispatch => {

        let observer = new Observer(
            response => {
                if (response.status == 200) {
                    dispatch({
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
        // console.log('ACTION')
        return dispatch({
            type: GET_INITIAL_MAPPINGS,
            payload: initialState.fieldMappings

        });
    }
};

export const reducer = (state = initialState, action) => {
    // console.log('REDUCER IMPORT', action);
    switch (action.type) {
        case FETCH_FIELD_MAPPINGS:
            // initialState.fieldMappings = action.payload;
            return {
                ...state,
                fieldMappings: action.payload
            };
        case SET_INITIAL_MAPPINGS:
            // initialState.fieldMappings = action.payload;
            return {
                ...state,
                fieldMappings: action.payload
            };
        case GET_INITIAL_MAPPINGS:
            return {
                ...state,
                fieldMappings: action.payload
            };
        default:
            return state;
    }
};
