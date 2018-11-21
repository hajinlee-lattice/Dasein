import React, { Component } from "../../../react-vendor";
import "./sort.scss";

export const DIRECTION_ASC = 'asc';
export const DIRECTION_DESC = 'desc';
export const DIRECTION_NONE = 'none';

export default class Sort extends Component {
  constructor(props) {
    super(props);
    this.state = { direction: this.props.direction };
    this.sortHandler = this.sortHandler.bind(this);
  }

  sortHandler() {
    let newDirection = DIRECTION_NONE;
    switch (this.state.direction) {
      case DIRECTION_DESC:
        newDirection = DIRECTION_ASC;
        break;
      case DIRECTION_ASC:
        newDirection = DIRECTION_DESC;
        break;
      default:
        newDirection = DIRECTION_ASC;
    }
    this.setState({ direction: newDirection }, () => {
        this.props.callback(this.props.colName, this.state.direction);
    });
  }

  getDirection() {
    switch (this.props.direction) {
      case DIRECTION_DESC:
        return DIRECTION_DESC;
      case DIRECTION_ASC:
        return DIRECTION_ASC;
      default:
        return DIRECTION_NONE;
    }
  }
  getSortIcon() {
    let direction = this.getDirection();
    let className = "fa fa-sort";
    if (direction != DIRECTION_NONE) {
      return `${className}-amount-${direction}`;
    } else {
      return className;
    }
  }

  render() {
    return (
      <div className="sort-container" onClick={this.sortHandler}>
        <i
          className={this.getSortIcon()}
          aria-hidden="true"
        />
      </div>
    );
  }
}
