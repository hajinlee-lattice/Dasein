import React, { Component } from 'common/react-vendor';
import LeHPanel from 'common/widgets/container/le-h-panel.js';
import './priority-element.component.scss';

export default class PriorityElementComponent extends Component {
    constructor(props) {
        super(props);
        this.onDragEndHandler = this.onDragEndHandler.bind(this);
        this.onDragStartHandler = this.onDragStartHandler.bind(this);
    }

    componentDidMount() {}
    componentWillUnmount() {}

    onDragStartHandler(event) {
        this.props.dragStart(event);
    }

    onDragEndHandler(event) {
        this.props.dragEnd(event);
    }

    render() {
        return (
            <li 
                className="draggable"
                data-id={this.props.priority}
                draggable="true"
                onDragEnd={this.onDragEndHandler}
                onDragStart={this.onDragStartHandler}>
                {this.props.item.display_name}
            </li>
        );
    }
}
