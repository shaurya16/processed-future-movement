import { signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ColumnPicker } from './column-picker';
import { ColumnPreferences } from './column-preferences';
import { DEFAULT_VISIBLE_COLUMN_IDS } from './report-columns';

function setup() {
  const columnPreferences = {
    visibleIds: signal([...DEFAULT_VISIBLE_COLUMN_IDS]),
    visibleColumns: signal([]),
    isVisible: vi.fn((id: string) => DEFAULT_VISIBLE_COLUMN_IDS.includes(id)),
    toggle: vi.fn(),
    reset: vi.fn(),
  };
  TestBed.configureTestingModule({
    imports: [ColumnPicker],
    providers: [{ provide: ColumnPreferences, useValue: columnPreferences }],
  });
  return columnPreferences;
}

function open(fixture: { nativeElement: HTMLElement }) {
  const toggle: HTMLButtonElement = fixture.nativeElement.querySelector(
    '[data-testid="column-picker-toggle"]',
  )!;
  toggle.click();
}

describe('ColumnPicker', () => {
  beforeEach(() => TestBed.resetTestingModule());

  it('renders one checkbox per column, grouped by COLUMN_GROUPS, reflecting isVisible', async () => {
    const columnPreferences = setup();
    const fixture = TestBed.createComponent(ColumnPicker);
    fixture.detectChanges();
    open(fixture);
    fixture.detectChanges();
    await fixture.whenStable();

    const checkboxes = fixture.nativeElement.querySelectorAll('input[type="checkbox"]');
    expect(checkboxes.length).toBe(17);

    const headings: HTMLParagraphElement[] = Array.from(
      fixture.nativeElement.querySelectorAll('p'),
    );
    expect(headings.length).toBeGreaterThanOrEqual(1);
    expect(headings.some((h) => h.textContent?.includes('Client'))).toBe(true);

    const symbolCheckbox: HTMLInputElement = fixture.nativeElement.querySelector(
      '[data-testid="column-symbol"]',
    );
    expect(symbolCheckbox.checked).toBe(columnPreferences.isVisible('symbol'));
  });

  it('clicking a checkbox toggles that column, not just the first one', async () => {
    setup();
    const fixture = TestBed.createComponent(ColumnPicker);
    fixture.detectChanges();
    open(fixture);
    fixture.detectChanges();
    await fixture.whenStable();

    const symbolCheckbox: HTMLInputElement = fixture.nativeElement.querySelector(
      '[data-testid="column-symbol"]',
    );
    symbolCheckbox.click();

    const columnPreferences = TestBed.inject(ColumnPreferences) as unknown as {
      toggle: ReturnType<typeof vi.fn>;
    };
    expect(columnPreferences.toggle).toHaveBeenCalledWith('symbol');
  });

  it('opens and closes the panel via the toggle button, updating aria-expanded', async () => {
    setup();
    const fixture = TestBed.createComponent(ColumnPicker);
    fixture.detectChanges();
    await fixture.whenStable();

    const toggleButton: HTMLButtonElement = fixture.nativeElement.querySelector(
      '[data-testid="column-picker-toggle"]',
    );
    expect(fixture.nativeElement.querySelector('[data-testid="column-symbol"]')).toBeNull();
    expect(toggleButton.getAttribute('aria-expanded')).toBe('false');

    toggleButton.click();
    fixture.detectChanges();
    await fixture.whenStable();
    expect(fixture.nativeElement.querySelector('[data-testid="column-symbol"]')).not.toBeNull();
    expect(toggleButton.getAttribute('aria-expanded')).toBe('true');

    toggleButton.click();
    fixture.detectChanges();
    await fixture.whenStable();
    expect(fixture.nativeElement.querySelector('[data-testid="column-symbol"]')).toBeNull();
    expect(toggleButton.getAttribute('aria-expanded')).toBe('false');
  });

  it('pressing Escape inside the open panel closes it', async () => {
    setup();
    const fixture = TestBed.createComponent(ColumnPicker);
    fixture.detectChanges();
    open(fixture);
    fixture.detectChanges();
    await fixture.whenStable();

    const panel: HTMLElement = fixture.nativeElement.querySelector(
      '[data-testid="column-symbol"]',
    ).closest('div')!;
    panel.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.querySelector('[data-testid="column-symbol"]')).toBeNull();
    const toggleButton: HTMLButtonElement = fixture.nativeElement.querySelector(
      '[data-testid="column-picker-toggle"]',
    );
    expect(toggleButton.getAttribute('aria-expanded')).toBe('false');
  });

  it('clicking Reset to defaults calls reset()', async () => {
    const columnPreferences = setup();
    const fixture = TestBed.createComponent(ColumnPicker);
    fixture.detectChanges();
    open(fixture);
    fixture.detectChanges();
    await fixture.whenStable();

    const resetButton: HTMLButtonElement = fixture.nativeElement.querySelector(
      '[data-testid="reset-columns"]',
    );
    resetButton.click();

    expect(columnPreferences.reset).toHaveBeenCalled();
  });
});
