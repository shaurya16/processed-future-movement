import { Component, OnInit, inject } from '@angular/core';
import { ReportService } from './report.service';

@Component({
  selector: 'app-report',
  imports: [],
  templateUrl: './report.html',
  styleUrl: './report.css',
})
export class Report implements OnInit {
  protected readonly reportService = inject(ReportService);

  ngOnInit(): void {
    this.reportService.load();
  }

  protected retry(): void {
    this.reportService.load();
  }

  protected refresh(): void {
    this.reportService.load();
  }
}
