const map = L.map('map').setView([37.7749, -122.4194], 13);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '&copy; OpenStreetMap contributors'
}).addTo(map);

// ====== Legend ======
const legend = L.control({ position: 'topright' });
legend.onAdd = function (map) {
  const div = L.DomUtil.create('div', 'info legend');
  const labels = [
    { color: 'lightblue', label: 'Empty' },
    { color: 'lightgreen', label: 'Few Riders' },
    { color: 'yellow', label: 'Several Riders' },
    { color: 'lightcoral', label: 'Many Riders' },
  ];
  div.innerHTML = labels.map(item =>
    `<div><span style="background:${item.color}; width: 16px; height: 16px; display:inline-block; margin-right:5px; border:1px solid #ccc;"></span>${item.label}</div>`
  ).join('');
  return div;
};
legend.addTo(map);
// =====================

function getOccupancyColor(vehicle) {
  switch (vehicle.occupancy_status) {
    case 0: return "lightblue";
    case 1: return "lightgreen";
    case 2: return "yellow";
    case 3: return "lightcoral";
    default:
      console.log(vehicle.route_id, vehicle.occupancy_status);
      return "lightgray";
  }
}

let vehicleMarkers = [];

function updateVehicles() {
  vehicleMarkers.forEach(marker => map.removeLayer(marker));
  vehicleMarkers = [];

  fetch("https://968885fb556c.ngrok-free.app/hot-data", {
    headers: { "ngrok-skip-browser-warning": "true" }
  })
    .then(res => res.json())
    .then(data => {
      data.forEach(vehicle => {
        if (!vehicle.route_id) return;
        const color = getOccupancyColor(vehicle);
        const labelIcon = L.divIcon({
          className: 'vehicle-label',
          html: `<div style="background-color:${color}; padding: 2px 4px; border-radius: 4px;">${vehicle.route_id || 'OOS'}</div>`,
          iconSize: null
        });
        const marker = L.marker([vehicle.latitude, vehicle.longitude], { icon: labelIcon }).addTo(map);
        vehicleMarkers.push(marker);
      });
    })
    .catch(err => console.error("Fetch error:", err));
}

let pollInterval;

document.addEventListener("visibilitychange", () => {
  if (document.hidden) {
    clearInterval(pollInterval);
  } else {
    updateVehicles();
    pollInterval = setInterval(updateVehicles, 30000);
  }
});

updateVehicles();
pollInterval = setInterval(updateVehicles, 30000);
