/* tslint:disable:no-unused-variable */
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { DebugElement } from '@angular/core';

import { PdComponent } from './pd.component';

describe('PdComponent', () => {
  let component: PdComponent;
  let fixture: ComponentFixture<PdComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ PdComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PdComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
