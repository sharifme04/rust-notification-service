import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ApiService } from '../../services/api.service';

interface Preferences {
  user_id: string;
  email: string | null;
  webhook_url: string | null;
}

@Component({
  selector: 'app-channel-config',
  standalone: true,
  imports: [CommonModule, FormsModule],
  template: `
    <div class="card">
      <h2>⚙️ Channel Configuration</h2>

      <div *ngIf="alert" class="alert" [class.alert-success]="alert.ok" [class.alert-error]="!alert.ok">
        {{ alert.msg }}
      </div>

      <form (ngSubmit)="save()" #f="ngForm">
        <div class="form-group">
          <label>Email address</label>
          <input type="email" [(ngModel)]="email" name="email" placeholder="user@example.com" />
        </div>
        <div class="form-group">
          <label>Webhook URL</label>
          <input type="url" [(ngModel)]="webhookUrl" name="webhookUrl" placeholder="https://example.com/hook" />
        </div>
        <div class="form-group">
          <label>Active channels</label>
          <div style="display:flex;gap:16px;margin-top:6px;">
            <label style="display:flex;align-items:center;gap:6px;font-size:0.875rem;color:#c9d1d9;">
              <input type="checkbox" [(ngModel)]="chEmail" name="chEmail" /> Email
            </label>
            <label style="display:flex;align-items:center;gap:6px;font-size:0.875rem;color:#c9d1d9;">
              <input type="checkbox" [(ngModel)]="chWebhook" name="chWebhook" /> Webhook
            </label>
            <label style="display:flex;align-items:center;gap:6px;font-size:0.875rem;color:#c9d1d9;">
              <input type="checkbox" [(ngModel)]="chInApp" name="chInApp" /> In-app
            </label>
          </div>
        </div>
        <button type="submit" class="btn btn-primary" [disabled]="saving">
          {{ saving ? 'Saving…' : 'Save preferences' }}
        </button>
      </form>
    </div>
  `,
})
export class ChannelConfigComponent implements OnInit {
  email = '';
  webhookUrl = '';
  chEmail = false;
  chWebhook = false;
  chInApp = true;
  saving = false;
  alert: { ok: boolean; msg: string } | null = null;

  constructor(private api: ApiService) {}

  ngOnInit(): void {
    this.api.getPreferences().subscribe({
      next: (p) => {
        this.email = p.email ?? '';
        this.webhookUrl = p.webhook_url ?? '';
      },
      error: () => {},
    });
  }

  save(): void {
    this.saving = true;
    this.alert = null;
    const channels: string[] = [];
    if (this.chEmail) channels.push('email');
    if (this.chWebhook) channels.push('webhook');
    if (this.chInApp) channels.push('in_app');

    this.api
      .upsertPreferences({
        email: this.email || null,
        webhook_url: this.webhookUrl || null,
        channels,
      })
      .subscribe({
        next: () => {
          this.saving = false;
          this.alert = { ok: true, msg: 'Preferences saved.' };
        },
        error: (err) => {
          this.saving = false;
          this.alert = { ok: false, msg: `Error: ${err.message}` };
        },
      });
  }
}
