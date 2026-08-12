import { Component, inject, signal } from '@angular/core';
import { ColumnPreferences } from './column-preferences';
import { COLUMN_GROUPS, ColumnGroup, REPORT_COLUMNS } from './report-columns';

@Component({
  selector: 'app-column-picker',
  template: `
    <div class="relative">
      <button
        type="button"
        data-testid="column-picker-toggle"
        class="rounded border border-rule px-2 py-1 text-sm text-ink-secondary hover:text-ink-primary"
        [attr.aria-expanded]="open()"
        (click)="open.set(!open())"
      >
        Columns ({{ columnPreferences.visibleIds().length }}) ▾
      </button>

      @if (open()) {
        <div
          class="absolute right-0 z-10 mt-1 max-h-80 w-64 overflow-y-auto rounded-lg border border-rule bg-surface-1 p-3 shadow-lg"
          (keydown.escape)="open.set(false)"
        >
          @for (group of groups; track group) {
            <p class="mt-2 text-xs font-semibold uppercase tracking-wide text-ink-muted first:mt-0">
              {{ group }}
            </p>
            @for (column of columnsIn(group); track column.id) {
              <label class="flex items-center gap-2 py-1 text-sm text-ink-primary">
                <input
                  type="checkbox"
                  [attr.data-testid]="'column-' + column.id"
                  [checked]="columnPreferences.isVisible(column.id)"
                  (change)="columnPreferences.toggle(column.id)"
                />
                {{ column.label }}
              </label>
            }
          }
          <button
            type="button"
            data-testid="reset-columns"
            class="mt-3 w-full rounded border border-rule py-1 text-sm text-ink-secondary hover:text-ink-primary"
            (click)="columnPreferences.reset()"
          >
            Reset to defaults
          </button>
        </div>
      }
    </div>
  `,
})
export class ColumnPicker {
  protected readonly columnPreferences = inject(ColumnPreferences);
  protected readonly open = signal(false);
  protected readonly groups = COLUMN_GROUPS;

  protected columnsIn(group: ColumnGroup) {
    return REPORT_COLUMNS.filter((column) => column.group === group);
  }
}
