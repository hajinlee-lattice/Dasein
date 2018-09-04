import './le-menu.scss';
import React, { Component } from 'react';

export default class LeMenu extends Component {
    constructor(props) {
        super(props);
        this.showMenu(false);

        this.state = { opened: false, name: this.props.name, outmain: false, outmenu: false }
        this.mouseMainEnterHandler = this.mouseMainEnterHandler.bind(this);
        this.mouseMenuEnterHandler = this.mouseMenuEnterHandler.bind(this);
        this.mouseMainLeaveHandler = this.mouseMainLeaveHandler.bind(this);
        this.mouseMenuLeaveHandler = this.mouseMenuLeaveHandler.bind(this);
    }

    getImage(image) {
        if (image) {
            const classes = `${image} item-img`;
            return <span className={classes}></span>
        } else {
            return '';
        }
    }
    getLabel(label) {
        if (label) {
            return <span className="label">{label}</span>
        } else {
            return null;
        }
    }
    mouseMainEnterHandler() {

        this.setState({
            outmain: false,
            outmenu: true
        }, () => {
            if (this.state.opened) {
                this.updateShowClass();
            }
        });
    }
    mouseMenuEnterHandler() {
        this.setState((prevState, props) => {
            return {
                outmain: true,
                outmenu: false
            }
        });
    }

    mouseMainLeaveHandler() {
        this.setState({
            outmain: true
        }, () => {
            this.updateShowClass();
        });
    }


    mouseMenuLeaveHandler() {
        this.setState({
            outmenu: true
        }, () => {
            this.updateShowClass();
        });

    }


    clickHandler(event) {
        this.setState((prevState, props) => {
            this.showMenu(!prevState.opened);
            return {
                opened: !prevState.opened,
                outmain: prevState.opened,
                outmenu: true

            }
        })
        if (this.props.callback) {
            this.props.callback(event, this.props.name);
        }
    }
    showMenu(show) {
        // console.log('show');
        if (show) {
            this.menuClass = `le-menu le-menu-show ${this.props.menuClass ? this.props.menuClass : ''}`;
        } else {
            this.menuClass = `le-menu ${this.props.menuClass ? this.props.menuClass : ''}`;

        }
    }

    updateShowClass() {
        // console.log('up', this.state);
        if (this.state.opened && (!this.state.outmain || !this.state.outmenu)) {
            this.showMenu(true);
            this.setState({ opened: true });
        } else {
            this.showMenu(false);
            this.setState({ opened: false });
        }
    }

    render() {

        return (
            <div className={this.menuClass} onClick={(event) => { this.clickHandler(event) }}
                onMouseEnter={this.mouseMainEnterHandler}
                onMouseLeave={this.mouseMainLeaveHandler}>
                <div className="menu-container">
                    {this.getImage(this.props.image)}
                    {this.getLabel(this.props.label)}
                </div>
                <div className={`menu-content ${this.props.menuClass ? this.props.menuClass : ''}`}
                    onMouseEnter={this.mouseMenuEnterHandler}
                    onMouseLeave={this.mouseMenuLeaveHandler} >
                    {this.props.children}
                </div>
            </div>

        )
    }
}