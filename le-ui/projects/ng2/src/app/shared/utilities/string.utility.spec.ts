/* tslint:disable:no-unused-variable */

import { TestBed, async, inject } from '@angular/core/testing';
import { StringUtility } from './string.utility';

describe('Service: String', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [StringUtility]
    });
  });

  it('should ...', inject([StringUtility], (service: StringUtility) => {
    expect(service).toBeTruthy();
  }));
});
