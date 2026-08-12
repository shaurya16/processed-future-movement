import { Component, inject } from '@angular/core';
import { ThemeStore } from './theme';

@Component({
  selector: 'app-theme-toggle',
  template: `
    <button
      type="button"
      data-testid="theme-toggle"
      class="rounded border border-rule px-2 py-1 text-sm text-ink-secondary hover:text-ink-primary"
      [attr.aria-label]="'Theme: ' + themeStore.theme() + '. Click to change.'"
      (click)="themeStore.cycle()"
    >
      {{ label() }}
    </button>
  `,
})
export class ThemeToggle {
  protected readonly themeStore = inject(ThemeStore);

  protected label(): string {
    const theme = this.themeStore.theme();
    if (theme === 'light') return 'Light';
    if (theme === 'dark') return 'Dark';
    return 'Auto';
  }
}
