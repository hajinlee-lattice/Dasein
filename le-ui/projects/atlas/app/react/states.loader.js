import states from 'atlas/import/templates/multiple/states';


const modulesStates = [];

function mergeStates(imported) {
    imported.forEach(state => {
        modulesStates.push(state);
    });
}

mergeStates(states);

export default modulesStates;