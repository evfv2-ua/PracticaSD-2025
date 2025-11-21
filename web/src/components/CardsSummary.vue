<template>
  <div class="grid cols-2">
    <div class="card">
      <div class="muted">Puntos de Carga</div>
      <h2>{{ cps.length }}</h2>
      <div class="stack" style="margin-top:8px;">
        <span class="badge status-ok">OK {{ countBy('ok') }}</span>
        <span class="badge status-faulty">Avería {{ countBy('faulty') }}</span>
        <span class="badge status-disconnected">Desconectado {{ countDisconnected }}</span>
      </div>
    </div>
    <div class="card">
      <div class="muted">Conductores</div>
      <h2>{{ drivers.length }}</h2>
      <div class="muted" style="margin-top:8px;">Sesiones activas: {{ activeSessions }}</div>
    </div>
  </div>
</template>

<script setup>
import { computed } from 'vue';
const props = defineProps({
  cps: { type: Array, default: () => [] },
  drivers: { type: Array, default: () => [] },
  sessions: { type: Array, default: () => [] },
});
const countBy = (status) => props.cps.filter(c => c.status === status).length;
const countDisconnected = computed(() => props.cps.filter(c => c.status === 'disconnected').length);
const activeSessions = computed(() => props.sessions.filter(s => s.state === 'running').length);
</script>
