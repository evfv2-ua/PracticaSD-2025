<template>
  <main class="content">
    <div class="hero" style="justify-content: space-between;">
      <div>
        <h1>EVCharging Central</h1>
        <div class="muted">Estado en tiempo real</div>
      </div>
      <div class="status-chip" :class="apiStatusClass">{{ apiStatusLabel }}</div>
    </div>

    <ControlBar
      @reload="load"
      @pauseAll="bulk('pause')"
      @resumeAll="bulk('resume')"
      @stopAll="bulk('stop')"
    />
    <CardsSummary :cps="cps" :drivers="drivers" :sessions="sessions" />

    <div class="grid equal-2" style="margin-top:12px;">
      <CpTable
        :cps="cps"
        @pause="pauseCp"
        @resume="resumeCp"
        @stop="stopCp"
        @refresh="load"
      />
      <div class="grid cols-1">
        <DriverTable :drivers="drivers" />
        <AssignCard :cps="cps" :drivers="drivers" @start="startAssign" @cancel="cancelAssign" />
      </div>
    </div>

    <div class="grid equal-2" style="margin-top:12px;">
      <SessionList :sessions="activeSessions" />
      <SessionsTable :sessions="sessions" />
    </div>

    <div v-if="error" class="card" style="margin-top:12px;color:#fca5a5;border-color:#ef4444;">
      {{ error }}
    </div>
    <div v-else-if="message" class="card success" style="margin-top:12px;">
      {{ message }}
    </div>
  </main>
</template>

<script setup>
import { onMounted, computed, onBeforeUnmount } from 'vue';
import { useDashboardStore } from './stores/useDashboardStore';
import ControlBar from './components/ControlBar.vue';
import CardsSummary from './components/CardsSummary.vue';
import CpTable from './components/CpTable.vue';
import DriverTable from './components/DriverTable.vue';
import SessionsTable from './components/SessionsTable.vue';
import AssignCard from './components/AssignCard.vue';
import SessionList from './components/SessionList.vue';

const store = useDashboardStore();
const cps = computed(() => store.cps);
const drivers = computed(() => store.drivers);
const sessions = computed(() => store.sessions);
const activeSessions = computed(() => store.activeSessions);
const error = computed(() => store.error);
const message = computed(() => store.message);
const apiStatus = computed(() => store.apiStatus);

const apiStatusClass = computed(() => apiStatus.value === 'ok' ? 'status-chip ok' : 'status-chip err');
const apiStatusLabel = computed(() => apiStatus.value === 'ok' ? 'Central OK' : 'Sin conexión');

const load = () => store.loadSnapshot();
const pauseCp = (cp_id) => store.command('pause', { cp_id });
const resumeCp = (cp_id) => store.command('resume', { cp_id });
const stopCp = (cp_id) => store.command('stop', { cp_id });
const bulk = (action) => store.command(action, { scope: 'all' });
const startAssign = ({ driver_id, cp_id }) => store.command('start', { driver_id, cp_id });
const cancelAssign = ({ driver_id, cp_id }) => store.command('cancel', { driver_id, cp_id });

onMounted(() => {
  store.startPolling();
});
onBeforeUnmount(() => {
  store.stopPolling();
});
</script>
