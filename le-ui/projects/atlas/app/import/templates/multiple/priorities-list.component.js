import React, { Component } from 'common/react-vendor';
import LeVPanel from 'common/widgets/container/le-v-panel.js';
import './priorities-list.component.scss';
import PriorityElementComponent from './priority-element.component';

export default class PrioritiesLitComponent extends Component {
    constructor(props) {
        super(props);
        this.state = { items: this.props.items };
        this.placeholder = document.createElement('li');
        this.placeholder.className = 'placeholder';
        this.dragStart = this.dragStart.bind(this);
        this.dragEnd = this.dragEnd.bind(this);
        this.dragOver = this.dragOver.bind(this);
    }

    componentDidMount() {}

    dragStart(e) {
        this.dragged = e.currentTarget;
        e.dataTransfer.effectAllowed = 'move';
        e.dataTransfer.setData('text/html', this.dragged);
    }

    dragEnd() {
        this.dragged.style.display = 'block';
        this.dragged.parentNode.removeChild(this.placeholder);

        // update state
        var data = this.props.items;
        var from = Number(this.dragged.dataset.id);
        var to = Number(this.over.dataset.id);
        if (from < to) {
            to--;
        }
        data.splice(to, 0, data.splice(from, 1)[0]);
        this.props.changeHandler(data);
        // this.setState({ items: data })
    }

    dragOver(e) {
        e.preventDefault();
        this.dragged.style.display = 'none';
        if (e.target.className === 'placeholder') return;
        this.over = e.target;
        e.target.parentNode.insertBefore(this.placeholder, e.target);
    }

    getListContainer() {
        if (this.props.loading == true) {
            return <p>Loading ...</p>;
        } else {
            var listItems = this.props.items.map((item, i) => {
                return (
                    <PriorityElementComponent
                        key={i}
                        item={item}
                        priority={i}
                        dragEnd={this.dragEnd}
                        dragStart={this.dragStart}/>
                );
            });
            return (
                <ul
                    className={`${"droppable"} ${this.props.saving ? 'disabled' : ''}`}
                    onDragOver={this.dragOver.bind(this)}
                >
                    {listItems}
                </ul>
            );
        }
    }

    render() {
        return (
            <LeVPanel hstretch={'true'} className={`${'dd-list'}`}>
                {this.getListContainer()}
            </LeVPanel>
        );
    }
}
