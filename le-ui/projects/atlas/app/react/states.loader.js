const modulesStates = [];
function mergeStates(imported) {
    if (imported) {
        imported.forEach(state => {
            modulesStates.push(state);
        });
    }
}

function importAll(r) {
    r.keys().forEach(key => {
        let state = r(key).default;
        if (state) {
            mergeStates(state);
        }
    });
}

importAll(require.context("../", true, /\.states.js$/));
export default modulesStates;