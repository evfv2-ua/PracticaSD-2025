// API de ejemplo; ajusta BASE_URL al host donde expongas la Central.
const BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export async function fetchSnapshot() {
  // Endpoint esperado: GET /snapshot devuelve { cps, drivers, sessions }
  const res = await fetch(`${BASE_URL}/snapshot`);
  if (!res.ok) throw new Error('No se pudo cargar snapshot');
  return res.json();
}

export async function sendCommand(action, payload = {}) {
  // Endpoint: POST /command { action, ...payload }
  const res = await fetch(`${BASE_URL}/command`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action, ...payload }),
  });
  if (!res.ok) throw new Error('No se pudo enviar comando');
  return res.json();
}
