<template>
  <div class="card topbar">
    <div>
      <h2 style="font-size:18px;margin-bottom:4px;">Panel Central</h2>
      <div class="muted">Vista de control en tiempo real</div>
    </div>
    <div class="stack">
      <button class="btn primary" @click="$emit('reload')">Refrescar</button>
      <button class="btn" @click="$emit('pauseAll')">Pausar todos</button>
      <button class="btn" @click="$emit('resumeAll')">Reanudar todos</button>
      <button class="btn danger" @click="$emit('stopAll')">Detener todos</button>
    </div>
  </div>
</template>
