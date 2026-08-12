import { TestBed } from '@angular/core/testing';
import { beforeEach, describe, expect, it } from 'vitest';
import { ColumnPreferences } from './column-preferences';
import { DEFAULT_VISIBLE_COLUMN_IDS, REPORT_COLUMNS } from './report-columns';

describe('ColumnPreferences', () => {
  beforeEach(() => {
    localStorage.clear();
    TestBed.resetTestingModule();
  });

  it('starts with the eight default columns', () => {
    const prefs = TestBed.inject(ColumnPreferences);

    expect(prefs.visibleIds()).toEqual(DEFAULT_VISIBLE_COLUMN_IDS);
    expect(prefs.visibleIds().length).toBe(8);
  });

  it('exposes seventeen definitions in total', () => {
    expect(REPORT_COLUMNS.length).toBe(17);
  });

  it('preserves declaration order regardless of toggle order', () => {
    const prefs = TestBed.inject(ColumnPreferences);

    prefs.toggle('Client_Information'); // last in declaration order
    prefs.toggle('clientType');         // third

    const ids = prefs.visibleColumns().map((c) => c.id);
    const declarationOrder = REPORT_COLUMNS.map((c) => c.id).filter((id) => ids.includes(id));
    expect(ids).toEqual(declarationOrder);
  });

  it('toggles a column off and on', () => {
    const prefs = TestBed.inject(ColumnPreferences);

    prefs.toggle('tradeCount');
    expect(prefs.isVisible('tradeCount')).toBe(false);

    prefs.toggle('tradeCount');
    expect(prefs.isVisible('tradeCount')).toBe(true);
  });

  it('persists across instances', () => {
    TestBed.inject(ColumnPreferences).toggle('grossShort');

    TestBed.resetTestingModule();
    expect(TestBed.inject(ColumnPreferences).isVisible('grossShort')).toBe(false);
  });

  it('falls back to defaults when stored ids are unknown', () => {
    // A future release renaming a column must not leave anyone with a broken table.
    localStorage.setItem('pfm.visibleColumns', JSON.stringify(['nope', 'alsoNope']));

    expect(TestBed.inject(ColumnPreferences).visibleIds()).toEqual(DEFAULT_VISIBLE_COLUMN_IDS);
  });

  it('drops unknown ids but keeps recognised ones', () => {
    localStorage.setItem('pfm.visibleColumns', JSON.stringify(['symbol', 'nope']));

    expect(TestBed.inject(ColumnPreferences).visibleIds()).toEqual(['symbol']);
  });

  it('falls back to defaults when the stored value is not an array', () => {
    localStorage.setItem('pfm.visibleColumns', JSON.stringify({ symbol: true }));

    expect(TestBed.inject(ColumnPreferences).visibleIds()).toEqual(DEFAULT_VISIBLE_COLUMN_IDS);
  });

  it('reset restores the defaults', () => {
    const prefs = TestBed.inject(ColumnPreferences);
    prefs.toggle('symbol');
    prefs.toggle('clientType');

    prefs.reset();

    expect(prefs.visibleIds()).toEqual(DEFAULT_VISIBLE_COLUMN_IDS);
  });
});
