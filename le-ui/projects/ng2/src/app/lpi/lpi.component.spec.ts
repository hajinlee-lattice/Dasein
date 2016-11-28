/* tslint:disable:no-unused-variable */
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { DebugElement } from '@angular/core';

import { LpiComponent } from './lpi.component';

describe('LpiComponent', () => {
  let component: LpiComponent;
  let fixture: ComponentFixture<LpiComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ LpiComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(LpiComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
