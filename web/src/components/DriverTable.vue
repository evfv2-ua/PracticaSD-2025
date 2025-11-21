<template>
  <div class="card">
    <div class="topbar">
      <div>
        <h3>Conductores</h3>
        <div class="muted">Solicitudes y estado actual</div>
      </div>
    </div>
    <table class="table">
      <thead>
        <tr>
          <th>Driver</th>
          <th>Estado</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="d in drivers" :key="d.driver_id">
          <td>{{ d.driver_id }}</td>
          <td><span class="badge" :class="statusClass(d.state)">{{ stateLabel(d) }}</span></td>
        </tr>
        <tr v-if="!drivers.length">
          <td colspan="2" class="muted">Sin conductores activos</td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script setup>
defineProps({
  drivers: { type: Array, default: () => [] },
});

const statusClass = (state) => ({
  CONECTADO: 'badge status-ok',
  CARGANDO: 'badge status-charging',
  DESCONECTADO: 'badge status-disconnected',
}[state] || 'badge status-disconnected');

const stateLabel = (d) => {
  if (d.state === 'CARGANDO' && d.cp_id) return `Cargando (${d.cp_id})`;
  if (d.state === 'CONECTADO') return 'Conectado';
  if (d.state === 'DESCONECTADO') return 'Desconectado';
  return d.state || 'Desconocido';
};
</script>
