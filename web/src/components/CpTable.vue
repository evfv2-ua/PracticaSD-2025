<template>
    <div class="card">
      <div class="topbar">
        <div>
          <h3>Puntos de carga</h3>
          <div class="muted">Controla estado y envía comandos</div>
        </div>
        <div class="stack">
          <button class="btn secondary" @click="$emit('refresh')">Refrescar</button>
        </div>
      </div>
      <table class="table">
        <thead>
          <tr>
            <th>CP</th>
            <th>Estado</th>
          <th>Último latido</th>
          <th>Acciones</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="cp in cps" :key="cp.cp_id">
          <td>{{ cp.cp_id }}</td>
          <td>
            <span class="badge" :class="statusClass(cp.status)">{{ statusLabel(cp.status, cp.driver_id) }}</span>
          </td>
          <td class="muted">{{ formatLastSeen(cp) }}</td>
          <td>
            <div class="stack">
              <button class="btn" @click="$emit('pause', cp.cp_id)" :disabled="disableControl(cp.status)">Pausar</button>
              <button class="btn" @click="$emit('resume', cp.cp_id)" :disabled="disableControl(cp.status)">Reanudar</button>
              <button class="btn danger" @click="$emit('stop', cp.cp_id)" :disabled="disableStop(cp.status)">Detener</button>
            </div>
          </td>
        </tr>
        <tr v-if="!cps.length">
          <td colspan="4" class="muted">Sin CPs registrados</td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script setup>
const props = defineProps({
  cps: { type: Array, default: () => [] },
});
const formatLastSeen = (cp) => {
  if (!cp.last_seen_delta && cp.last_seen_delta !== 0) return 'n/a';
  return `${cp.last_seen_delta.toFixed(1)}s`;
};
const disableControl = (status) => status === 'DESCONECTADO' || status === 'AVERIA';
const disableStop = (status) => status === 'DESCONECTADO';

const statusClass = (state) => ({
  CONECTADO: 'badge status-ok',
  CARGANDO: 'badge status-charging',
  AVERIA: 'badge status-faulty',
  DESCONECTADO: 'badge status-disconnected',
}[state] || 'badge status-disconnected');

const statusLabel = (state, driver) => {
  if (state === 'CARGANDO' && driver) return `Cargando (${driver})`;
  return {
    CONECTADO: 'Conectado',
    CARGANDO: 'Cargando',
    AVERIA: 'Avería',
    DESCONECTADO: 'Desconectado',
  }[state] || state;
};
</script>
