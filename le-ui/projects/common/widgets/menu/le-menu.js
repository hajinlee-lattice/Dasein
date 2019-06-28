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
        this.mouseMainOverHandler = this.mouseMainOverHandler.bind(this);
    }

    componentDidMount() {
        document.addEventListener('mousedown', this.handleClick);
    }

    componentWillUnmount() {
        document.removeEventListener('mousedown', this.handleClick);
    }

    getContent() {
        if (this.props.image) {
            const classes = `${this.props.image} item-img`;
            return <span className={classes}>{this.getLabel()}</span>
        } else {
            return this.getLabel();
        }
    }
    getLabel() {
        if (this.props.label) {
            return <span className="label">{this.props.label}</span>
        } else {
            return null;
        }
    }
    
    mouseMainOverHandler(){
        this.setState({
            outmain: false
        }, () => {
            if (this.state.opened) {
                this.updateShowClass();
            }
        });
    }

    mouseMainEnterHandler() {
        this.setState({
            outmain: false
        }, () => {
            if (this.state.opened) {
                this.updateShowClass();
            }
        });
    }
    mouseMenuEnterHandler() {
        this.setState((prevState, props) => {
            return {
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
        setTimeout(() => {
            this.setState({
                outmenu: true
            }, () => {
                this.updateShowClass();
            });
        }, 2000);
    }

    handleClick = (e)  => {
        if (this.node.contains(e.target)) {
            return;
        }
        this.handleClickOutside();
    }

    handleClickOutside() {
        this.setState({
            outmain: true
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
        if (show) {
            this.menuClass = `le-menu le-menu-show ${this.props.menuClass ? this.props.menuClass : ''}`;
        } else {
            this.menuClass = `le-menu ${this.props.menuClass ? this.props.menuClass : ''}`;

        }
    }

    updateShowClass() {
        if (this.state.opened && (!this.state.outmain || !this.state.outmenu)) {
            this.showMenu(true);
            this.setState({ opened: true, outmain:false, outmenu: true });
        } else {
            this.showMenu(false);
            this.setState({ opened: false, outmain:true, outmenu: true });
        }
    }

    render() {
        this.menuClass = `le-menu ${(this.state.opened && (!this.state.outmain || !this.state.outmenu)) ? 'le-menu-show' : ''} ${this.props.menuClass ? this.props.menuClass : ''}`;
        return (
            <div className={this.menuClass} onClick={(event) => { this.clickHandler(event) }}
                onMouseEnter={this.mouseMainEnterHandler}
                onMouseOver={this.mouseMainOverHandler}
                ref={node => this.node = node}>
                <div className="menu-container">
                    {this.getContent()}
                </div>
                <div className={`menu-content ${this.props.menuClass ? this.props.menuClass : ''}`}
                    onMouseEnter={this.mouseMenuEnterHandler}>
                    {this.props.children}
                </div>
            </div>

        )
    }
}