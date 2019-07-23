import modulesStates from "atlas/react/states.loader";
const appState = {
	name: "home",
	abstract: true
};

const mainState = [appState];
// console.log(modulesStates);
modulesStates.forEach(moduleStates => {
	mainState.push(moduleStates);
});

export default mainState;
