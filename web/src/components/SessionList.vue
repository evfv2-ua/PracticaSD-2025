<template>
  <div class="card">
    <div class="topbar">
      <div>
        <h3>Cargas en curso</h3>
        <div class="muted">Estado en tiempo real</div>
      </div>
    </div>
    <div v-if="!activeSessions.length" class="muted">Sin cargas activas</div>
    <div v-for="s in activeSessions" :key="s.session_id" style="margin-bottom:12px;">
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          <div style="font-weight:600;">{{ s.driver_id }} @ {{ s.cp_id }}</div>
          <div class="muted">kWh {{ s.energy_kwh ?? 0 }} · € {{ s.cost ?? 0 }}</div>
        </div>
        <div class="muted">{{ percent(s) }}%</div>
      </div>
      <div class="progress" style="margin-top:6px;">
        <div class="progress-bar" :style="{ width: percent(s) + '%' }"></div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed } from 'vue';

const props = defineProps({
  sessions: { type: Array, default: () => [] },
});
const battery = 50; // kWh simulada
const activeSessions = computed(() => props.sessions.filter(s => s.state === 'running'));
const percent = (s) => {
  const energy = s.energy_kwh || 0;
  const pct = Math.min(100, (energy / battery) * 100);
  return pct.toFixed(1);
};
</script>
