<template>
  <div class="card">
    <div class="topbar">
      <div>
        <h3>Sesiones</h3>
        <div class="muted">Actividad reciente</div>
      </div>
    </div>
    <table class="table">
      <thead>
        <tr>
          <th>ID</th>
          <th>CP</th>
          <th>Driver</th>
          <th>Estado</th>
          <th>kWh</th>
          <th>Coste (€)</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="s in sessions" :key="s.session_id">
          <td>{{ s.session_id }}</td>
          <td>{{ s.cp_id }}</td>
          <td>{{ s.driver_id }}</td>
          <td class="muted">{{ s.state }}</td>
          <td>{{ s.energy_kwh ?? '-' }}</td>
          <td>{{ s.cost ?? '-' }}</td>
        </tr>
        <tr v-if="!sessions.length">
          <td colspan="6" class="muted">Sin sesiones</td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script setup>
defineProps({
  sessions: { type: Array, default: () => [] },
});
</script>
