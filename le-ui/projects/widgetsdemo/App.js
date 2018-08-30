import React, { Component } from 'react';
import { UIRouter, UIView } from '@uirouter/react';
import './App.css'
// import NavigationBar from './modules/navigation';
import LeToolbar, {HORIZONTAL, VERTICAL} from '../common/widgets/toolbar/le-toolbar';
import LeToolbarItem from '../common/widgets/toolbar/le-toolbar-item';

import router from './router';

class App extends Component {

    render() {
        return (
            <div className="main">
                <LeToolbar direction={VERTICAL}>
                    {/* <img src="../common/assets/images/bkg-active-model-account.png"></img>     */}
                    <LeToolbarItem image="fa fa-user" 
                                    label="B" 
                                    name="button"
                                    callback= {(name) => {
                                        console.log('NAME ',name);
                                        router.stateService.go('buttons') 
                                    }}/>
                    <LeToolbarItem  
                                    image="fa fa-calendar-check-o" 
                                    name="dropdown"
                                    label="D" 
                                    callback= {(name) => {
                                        console.log('NAME ',name);
                                        router.stateService.go('dropdowns') 
                                    }}/>
                    <LeToolbarItem image="fa fa-ellipsis-v" 
                                    label="M" 
                                    name="menus"
                                    callback= {(name) => {
                                        console.log('NAME ',name);
                                        router.stateService.go('menus') 
                                    }}/>
                
                </LeToolbar>
                
                <UIRouter router={router}>
                    <UIView />
                </UIRouter>
            </div>
        );
    }
}

export default App;