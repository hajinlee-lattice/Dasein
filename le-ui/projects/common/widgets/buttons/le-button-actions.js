import React, { Component } from "../../react-vendor";
import "./button-actions.scss";
import PropTypes from "prop-types";
import LeMenuItem from "../menu/le-menu-item";

export default class LeButtonActions extends Component {
  constructor(props) {
    super(props);
    this.state = { opened: false, name: this.props.name, label:  this.props.config.label };
    this.selectActionClickHandler = this.selectActionClickHandler.bind(this);
    this.menuClass = "menu-content";
    this.buttonClass = "";
  }

  selectActionClickHandler(event, nameItem) {
    // console.log("Clicked", event, nameItem);
    // console.log('+++++> ',this.props.config.actions[nameItem]);
    this.setState((prevState, props) => {
      this.showMenu(!prevState.opened);
      console.log(props);
      return {
        opened: !prevState.opened,
        label: this.props.config.actions[nameItem] ? this.props.config.actions[nameItem] : prevState.label 
      };
    });
  };

  applyActionClickHandler(){

  }
  
  showMenu(show) {
    console.log("show", show);
    if (show) {
      this.menuClass = `menu-content le-menu-show ${
        this.props.menuClass ? this.props.menuClass : ""
      }`;
      this.buttonClass = "menu-opened";
    } else {
      this.menuClass = `menu-content ${
        this.props.menuClass ? this.props.menuClass : ""
      }`;
      this.buttonClass = "";
    }
  };

  render() {
    return (
      <div className="button-actions-container">
        <button
          className={`button-actions button-label ${this.buttonClass}`}
          onClick={this.selectActionClickHandler}
        >
          <span>{this.state.label}</span>
        </button>
        <button className={`button-actions actions-drop ${this.buttonClass}`}>
          <span className={this.props.config.image} />
        </button>
        <ul className={this.menuClass}>
          {React.Children.map(this.props.children, (menuItem, index) => {
            return (
              <LeMenuItem
                name={menuItem.props.name}
                label={menuItem.props.label}
                image={menuItem.props.image}
                callback={this.selectActionClickHandler.bind(
                  this,
                  null,
                  menuItem.props.name
                )}
              />
            );
          })}
        </ul>
      </div>
    );
  }
}
LeButtonActions.propTypes = {};
