/* tslint:disable:no-unused-variable */

import { TestBed, async, inject } from '@angular/core/testing';
import { StorageUtility } from './storage.utility';

describe('Service: StorageUtility', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [StorageUtility]
    });
  });

  it('should ...', inject([StorageUtility], (service: StorageUtility) => {
    expect(service).toBeTruthy();
  }));
});
