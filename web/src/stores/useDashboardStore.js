import { defineStore } from 'pinia';
import { fetchSnapshot, sendCommand } from '../utils/api';

export const useDashboardStore = defineStore('dashboard', {
  state: () => ({
    cps: [],
    drivers: [],
    sessions: [],
    activeSessions: [],
    loading: false,
    error: null,
    message: null,
    apiStatus: 'unknown',
    _timer: null,
  }),
  actions: {
    async loadSnapshot() {
      this.loading = true;
      this.error = null;
      this.message = null;
      try {
        const data = await fetchSnapshot();
        this.cps = data.cps || [];
        this.drivers = data.drivers || [];
        this.sessions = data.sessions || [];
        this.activeSessions = data.active_sessions || [];
        this.apiStatus = 'ok';
      } catch (err) {
        this.error = err.message || 'Error cargando snapshot';
        this.apiStatus = 'error';
      } finally {
        this.loading = false;
      }
    },
    async command(action, payload = {}) {
      try {
        this.error = null;
        this.message = null;
        await sendCommand(action, payload);
        this.message = 'Comando enviado';
        await this.loadSnapshot();
      } catch (err) {
        this.error = err.message || 'Error enviando comando';
      }
    },
    startPolling(intervalMs = 4000) {
      if (this._timer) clearInterval(this._timer);
      this.loadSnapshot();
      this._timer = setInterval(() => this.loadSnapshot(), intervalMs);
    },
    stopPolling() {
      if (this._timer) clearInterval(this._timer);
      this._timer = null;
    },
  },
});
