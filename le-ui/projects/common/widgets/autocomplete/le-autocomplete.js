import React, { Component } from "common/react-vendor";
import "./le-autocomplete.scss";
import search from "./le-search.utils";

class LeAutocomplete extends Component {
  constructor(props) {
    super(props);
    this.removeChip = this.removeChip.bind(this);
    this.addChip = this.addChip.bind(this);
    this.keyPressedHandler = this.keyPressedHandler.bind(this);
    this.removeChip = this.removeChip.bind(this);
    this.getItem = this.getItem.bind(this);
    this.getChipsSelected = this.getChipsSelected.bind(this);

    this.itemsAvailable = [];
    this.state = {
      value: "",
      chipsSelected: [],
      idSelected: {},
      show: false,
      selected: 0
    };
  }

  removeChip(item, position) {
    let tmpChips = [...this.state.chipsSelected];
    tmpChips.splice(position, 1);
    if(this.props.callback){
      this.props.callback(tmpChips);
    }
    const idsCopy = { ...this.state.idSelected };
    delete idsCopy[item.id];
    this.setState({ chipsSelected: tmpChips, idSelected: idsCopy });
    
  }

  getItem(item, index) {
    return (
      <li
        key={index}
        className={`${"le-autocomplete-list-item"} ${
          this.state.selected == index ? "selected" : ""
        }`}
        onClick={event => {
          //.log("Clicked");
          this.addChip(item, index);
        }}
      >
        {item.displayName}
      </li>
    );
  }

  getListItems() {
    let tmp = search(this.props.listItems, this.state.value, "displayName");
    let retUI = [];
    this.itemsAvailable = [];
    let id = 0;
    tmp.forEach((element, index) => {
      if (!this.state.idSelected[element.id]) {
        this.itemsAvailable.push(element);
        retUI.push(this.getItem(element, id));
        id++;
      }
    });
    return retUI;
  }
  keyPressedHandler(event) {
    event.stopPropagation();
    switch (event.keyCode) {
      case 40:
        //ArrowDown
        if (this.state.selected + 1 < this.itemsAvailable.length) {
          this.setState({ selected: this.state.selected + 1 });
        }
        break;
      case 38:
        //ArrowUp
        if (this.state.selected - 1 >= 0) {
          this.setState({ selected: this.state.selected - 1 });
        }
        break;
      case 27:
        //Escape
        this.setState({ show: false });
        break;

      case 8:
        //Backspace
        if (this.state.value == "") {
          this.setState({ show: false });
        }
      case 32:
        //Space with ctrl
        if (event.ctrlKey) {
          this.setState({ selected: 0, show: true });
        }
        break;

      case 13:
        let item = this.itemsAvailable[this.state.selected];
        let indexItem = this.state.selected;
        this.addChip(item, indexItem);
        this.setState({ selected: 0, show: false });
        break;
    }
  }
  searchItems(event) {
    let val = event.target.value;
    this.setState({ value: val, show: true });
  }

  addChip(item, index) {
    let tmpChips = [...this.state.chipsSelected];
    tmpChips.push(item);
    const idsCopy = { ...this.state.idSelected };
    idsCopy[item.id] = item.name;

    this.setState({
      chipsSelected: tmpChips,
      idSelected: idsCopy,
      selected: 0
    });
    if(this.props.callback){
      this.props.callback(tmpChips);
    }
  }

  getChipsSelected() {
    let chipsUI = this.state.chipsSelected.map((item, index) => {
      return (
        <li key={index} className="le-autocomplete">
          <span className="le-autocomplete-name">{item.displayName}</span>
          <i
            className="le-autocomplete-remove fa fa-times"
            aria-hidden="true"
            onClick={() => {
              this.removeChip(item, index);
            }}
          />
        </li>
      );
    });
    return chipsUI;
  }

  render() {
    return (
      <div className="le-autocomplete">
        <ul className="le-autocomplete-selected">{this.getChipsSelected()}</ul>
        <div className="le-autocomplete-input">
          <i className="fa fa-search" aria-hidden="true" />
          <input
            type="text"
            ref={input => {
              this.chipsInput = input;
            }}
            value={this.state.value}
            onKeyUp={event => {
              this.keyPressedHandler(event);
            }}
            onChange={event => {
              this.searchItems(event);
            }}
            onBlur={event => {
              setTimeout(() => {
                this.setState({ show: false });
              }, 200);
            }}
          />
          <div
            className={`${"le-autocomplete-dropdown"} ${
              this.state.show != "" ? "le-autocomplete-dropdown-show" : ""
            }`}
          >
            <ul
              className={`${"le-autocomplete-dropdown-content"} ${
                this.state.show != "" ? "le-autocomplete-dropdown-show" : ""
              }`}
            >
              {this.getListItems()}
            </ul>
          </div>
        </div>
      </div>
    );
  }
}

export default LeAutocomplete;
