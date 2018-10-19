import React, { Component, UIRouter, UIView } from '../../../common/react-vendor';


import router from './router';
export default class MultipleViews extends Component{
    constructor(props) {
        super(props);
        this.goToView = this.goToView.bind(this);
    }

    goToView(){
        console.log('The router');
        router.stateService.go('list.buttons');
    }

    render() {
        return(
            <div>
                <h1>List of components</h1>
                <button onClick={this.goToView}>Menu</button>

                
            </div>
        )
    }

}