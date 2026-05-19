import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpClient } from '@angular/common/http';

interface MetricSnapshot {
  notifications_created_total: number;
  deliveries_succeeded_total: number;
  deliveries_failed_total: number;
  active_ws_connections: number;
}

@Component({
  selector: 'app-metrics-panel',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div class="card">
      <h2>📊 Live Metrics <span style="font-size:0.7rem;color:#484f58;">(auto-refresh 10 s)</span></h2>
      <div class="metric-grid">
        <div class="metric-box">
          <div class="metric-value">{{ metrics.notifications_created_total }}</div>
          <div class="metric-label">Notifications created</div>
        </div>
        <div class="metric-box">
          <div class="metric-value" style="color:#3fb950;">{{ metrics.deliveries_succeeded_total }}</div>
          <div class="metric-label">Delivered</div>
        </div>
        <div class="metric-box">
          <div class="metric-value" style="color:#f85149;">{{ metrics.deliveries_failed_total }}</div>
          <div class="metric-label">Failed</div>
        </div>
        <div class="metric-box">
          <div class="metric-value" style="color:#e3b341;">{{ metrics.active_ws_connections }}</div>
          <div class="metric-label">WS connections</div>
        </div>
        <div class="metric-box">
          <div class="metric-value" style="font-size:1.2rem;">{{ successRate }}%</div>
          <div class="metric-label">Success rate</div>
        </div>
      </div>
      <div *ngIf="lastUpdated" style="font-size:0.75rem;color:#484f58;margin-top:12px;">
        Last updated: {{ lastUpdated | date:'HH:mm:ss' }}
      </div>
    </div>
  `,
})
export class MetricsPanelComponent implements OnInit, OnDestroy {
  metrics: MetricSnapshot = {
    notifications_created_total: 0,
    deliveries_succeeded_total: 0,
    deliveries_failed_total: 0,
    active_ws_connections: 0,
  };
  lastUpdated: Date | null = null;
  private intervalId: ReturnType<typeof setInterval> | null = null;

  get successRate(): string {
    const total =
      this.metrics.deliveries_succeeded_total + this.metrics.deliveries_failed_total;
    if (total === 0) return '—';
    return ((this.metrics.deliveries_succeeded_total / total) * 100).toFixed(1);
  }

  constructor(private http: HttpClient) {}

  ngOnInit(): void {
    this.fetch();
    this.intervalId = setInterval(() => this.fetch(), 10_000);
  }

  ngOnDestroy(): void {
    if (this.intervalId) clearInterval(this.intervalId);
  }

  private fetch(): void {
    this.http.get('/api/v1/metrics/summary', { responseType: 'text' }).subscribe({
      next: (raw) => {
        this.metrics = this.parsePrometheus(raw);
        this.lastUpdated = new Date();
      },
      error: () => {},
    });
  }

  private parsePrometheus(text: string): MetricSnapshot {
    const get = (name: string): number => {
      const match = new RegExp(`^${name}\\s+(\\S+)`, 'm').exec(text);
      return match ? parseFloat(match[1]) : 0;
    };
    return {
      notifications_created_total: get('notifications_created_total'),
      deliveries_succeeded_total: get('deliveries_succeeded_total'),
      deliveries_failed_total: get('deliveries_failed_total'),
      active_ws_connections: get('active_ws_connections'),
    };
  }
}
