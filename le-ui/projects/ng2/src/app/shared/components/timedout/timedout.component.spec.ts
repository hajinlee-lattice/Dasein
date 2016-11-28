/* tslint:disable:no-unused-variable */
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { DebugElement } from '@angular/core';

import { TimedoutComponent } from './timedout.component';

describe('TimedoutComponent', () => {
  let component: TimedoutComponent;
  let fixture: ComponentFixture<TimedoutComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TimedoutComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TimedoutComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
