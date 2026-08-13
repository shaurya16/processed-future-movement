import { Component, OnInit, effect, inject } from '@angular/core';
import { ReportService } from './report.service';
import { IngestionStatusService } from './ingestion-status.service';
import { SourceFilePanel } from './source-file-panel';
import { KpiRow } from './kpi-row';
import { FilterBar } from './filter-bar';
import { ColumnPicker } from './column-picker';
import { RefreshControl } from './refresh-control';
import { ReportTable } from './report-table';
import { ThemeToggle } from '../shared/theme-toggle';

@Component({
  selector: 'app-report',
  imports: [
    SourceFilePanel,
    KpiRow,
    FilterBar,
    ColumnPicker,
    RefreshControl,
    ReportTable,
    ThemeToggle,
  ],
  templateUrl: './report.html',
})
export class Report implements OnInit {
  protected readonly reportService = inject(ReportService);
  private readonly statusService = inject(IngestionStatusService);

  constructor() {
    // Ingestion is triggered out of band (POST :8081/api/v1/ingest, deliberately
    // not routable from the UI), so the run that fills the table usually happens
    // after this component has rendered. Loading provenance once on init left the
    // panel asserting "Not yet ingested." forever while rows appeared beside it.
    //
    // lastLoadedAt ticks on every successful report fetch -- initial load, poll
    // tick and manual refresh alike -- so the panel inherits the auto-refresh
    // toggle and the visibility-change pause for free. The effect never reads
    // statusService.status(), so writing it cannot re-trigger this effect.
    effect(() => {
      this.reportService.lastLoadedAt();
      this.statusService.load();
    });
  }

  ngOnInit(): void {
    this.reportService.load();
    this.reportService.startPolling();
  }

  protected retry(): void {
    this.reportService.load();
  }
}
