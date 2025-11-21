<template>
  <div class="card" style="margin-top:16px;">
    <div class="topbar">
      <div>
        <h3>Asignar carga</h3>
        <div class="muted">Selecciona driver y CP para lanzar START, o cancela una carga activa.</div>
      </div>
    </div>
    <div class="grid cols-2">
      <div>
        <label class="muted">Driver</label>
        <select v-model="driverId" class="input">
          <option value="">Selecciona driver</option>
          <option v-for="d in drivers" :key="d.driver_id" :value="d.driver_id">{{ d.driver_id }} ({{ d.state || 'idle' }})</option>
        </select>
      </div>
      <div>
        <label class="muted">CP (solo conectados)</label>
        <select v-model="cpId" class="input">
          <option value="">Selecciona CP</option>
          <option v-for="c in cpActivos" :key="c.cp_id" :value="c.cp_id">{{ c.cp_id }} ({{ statusLabel(c.status) }})</option>
        </select>
      </div>
    </div>
    <div class="stack" style="margin-top:12px;">
      <button class="btn primary" @click="start" :disabled="!driverId || !cpId">Iniciar carga</button>
      <button class="btn danger" @click="cancel" :disabled="!driverId">Cancelar carga</button>
    </div>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue';

const props = defineProps({
  cps: { type: Array, default: () => [] },
  drivers: { type: Array, default: () => [] },
});
const emits = defineEmits(['start', 'cancel']);
const driverId = ref('');
const cpId = ref('');
const cpActivos = computed(() => props.cps.filter(c => c.status === 'CONECTADO'));

const start = () => {
  if (!driverId.value || !cpId.value) return;
  emits('start', { driver_id: driverId.value, cp_id: cpId.value });
};
const cancel = () => {
  if (!driverId.value) return;
  emits('cancel', { driver_id: driverId.value, cp_id: cpId.value || undefined });
};

const statusLabel = (state) => ({
  CONECTADO: 'Conectado',
  AVERIA: 'Avería',
  CARGANDO: 'Cargando',
  DESCONECTADO: 'Desconectado',
}[state] || state);
</script>

<style scoped>
.input {
  width: 100%;
  padding: 10px;
  border-radius: 10px;
  border: 1px solid #1f2a44;
  background: #0b1221;
  color: #e8ecf8;
  margin-top: 6px;
}
select {
  appearance: none;
}
label {
  display: block;
  margin-bottom: 2px;
}
</style>
