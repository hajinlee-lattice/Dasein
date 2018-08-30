import './le-menu-button.scss';
import React, { Component } from 'react';


class LeMenuButton extends Component {

    constructor(props) {
        super(props);
        console.log('PROPS', props);
        this.buttonClickHandler = this.buttonClickHandler.bind(this);
        this.state = { opened: false, closedicon: 'fa fa-angle-down'};
        this.showMenu = '';
    }

    getPrimaryInfo(){
        if(this.props.config && this.props.config.primaryInfo){
            return <li className="main-info">{this.props.config.primaryInfo}</li>
        }else {
            return null;
        }
    }

    getSecondaryInfo(){
        if(this.props.config && this.props.config.secondaryInfo){
            return <li className="secondary-info">{this.props.config.secondaryInfo}</li>
        }else {
            return null;
        }
    }

    updateShowMenu() {
        this.showMenu = `${!this.state.opened ? 'showMenu' : ''}`

        this.setState((prevState, props) => {
            return {
                opened: !prevState.opened,
                closedicon: !prevState.opened ? 'fa fa-angle-up' : 'fa fa-angle-down'
            }
        });
    }
    

    buttonClickHandler() {
        this.updateShowMenu();
    }
    
    mouseHandler(){

    }

    render() {
        return (
            <div className={`le-menu-button ${this.showMenu}`}>
                <div className="le-menu-button-container" onClick={this.buttonClickHandler}>
                    <div className="button-container">
                        <div className="hcontainer">
                            <span className="fa fa-user fa-lg icon-container"></span>
                            <ul className="info-container">
                                {this.getPrimaryInfo()}
                                {this.getSecondaryInfo()}
                            </ul>
                        </div>
                        <span className={`tool-icon ${this.state.closedicon}`}></span>
                    </div>
                    <div className={`menu-content ${this.showMenu}`}>
                        {this.props.children}
                    </div>
                </div>
            </div>
        )
    }
}

export default LeMenuButton;