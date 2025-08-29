

// ===== GLOBAL VARS =====
let route_filtered = false;
let route_filtered_list = [];
let type_filtered = false;
let type_filtered_list = [];
let num_active_vehicles = 0;
let routeCounts = {};

// ===== MAP SETUP =====
    // L is Leaflet object imported with the js code for Leaflet
    // 'map' determines in which HTML element id the map should appear
const map = L.map('map').setView([37.7749, -122.447], 12);
    // tileLayer is the style of map, more options available at 
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    // attribution is just giving credit to the source
    attribution: '&copy; OpenStreetMap contributors'
}).addTo(map);

// HEADY THEME LOL
// L.tileLayer('https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png', {
//     attribution: '© OpenStreetMap, © OpenTopoMap',
//     maxZoom: 17
// }).addTo(map);

// ===== LEGEND =====  
    // L.control create UI overlay on map
const legend = L.control({ position: 'topright' });
    // onAdd is run once the UI control is placed on the map
    // 'map' is the const map from above -- defines which map to add to
legend.onAdd = function (map) {
    // how Leaflet creates <div class="info legend"></div>
    const legend_div = L.DomUtil.create('div', 'info legend');

    // define colors for legend -- will be used in for loop to build HTML
    const labels = [
        {color: 'lightblue', text: 'Empty'},
        {color: 'lightgreen', text: 'Few Riders'},
        {color: 'yellow', text: 'Several Riders'},
        {color: 'lightcoral', text: 'Many Riders'},
    ];

    // innerHTML allows you to set HTML for inside the element -- in this legend_div
    legend_div.innerHTML = labels.map(item => {
        return `<div><span style="background:${item.color}; width: 16px; height: 16px; display:inline-block; margin-right:5px; border:1px solid #ccc;"></span>${item.text}</div>`;
    }).join('');

    return legend_div;
};
legend.addTo(map);

// ===== VEHICLE DATA =====
let vehicleMarkers = [];

// used to dynamically determine the vehicles color during mapping
function getOccupancyColor(vehicle) {
    switch (vehicle.occupancy) {
      case 0:
        return "lightblue";
      case 1:
        return "lightgreen";
      case 2:
        return "yellow";
      case 3:
        return "lightcoral";
      default:
        console.log(vehicle.route_id, vehicle.occupancy);
        return "lightgray";  // fallback for undefined or unexpected values
    }
}
// fetches vehicle json data from API and puts it on map
function updateVehicles() {
    // removes markers (in vehicleMarkers list) from map
    vehicleMarkers.forEach(marker => map.removeLayer(marker));
    // clears vehicleMarkers, so it only contains current markers
    vehicleMarkers = [];
    // clears routes to display
    routeCounts = {};
    // grab json object from API
    fetch("https://968885fb556c.ngrok-free.app/hot-data", {
        // tell auto-fetch it can skip ngrok's verification header
        headers: {
            "ngrok-skip-browser-warning": "true"
        }
    // then commands wait for the previous function call to return before running
    }).then(response => response.json())
      .then(data => {
        // now vehicle is a json object as defined when pushed to API
        data.forEach(vehicle => {
            if (!vehicle.route_id) return;

            // PLACE HOLDER FOR FILTERING LOGIC
            if (route_filtered && route_filtered_list.length > 0) {
                if (!route_filtered_list.includes(String(vehicle.route_id).toUpperCase())) {
                    return; // Skip this vehicle
                }
            }

            // keep track of count of vehicle per route
            if (routeCounts[vehicle.route_id]) {
                routeCounts[vehicle.route_id].count++;
            } else {
                routeCounts[vehicle.route_id] = {
                    count: 1,
                    name: vehicle.route_name,
                    color: vehicle.route_color
                };
            }

            // get vehicle color
            const color = getOccupancyColor(vehicle);

            // define appearance of marker
            const vehicle_icon = L.divIcon({
                className: 'vehicle-label',
                html: `<div style="background-color:${color}; padding: 2px 4px; border-radius: 10px;">${vehicle.route_id}</div>`,
                iconSize: null
            });

            // define marker and add to map
            const marker = L.marker([vehicle.lat, vehicle.lon], { icon: vehicle_icon }).addTo(map);
            
            // add vehicle marker to vehicleMarkers
            vehicleMarkers.push(marker)

            // count active busses (within filter) and set on html page
            num_active_vehicles = vehicleMarkers.length;
            document.getElementById('bus-count').textContent = num_active_vehicles;
        });
        num_active_routes = Object.keys(routeCounts).length;
        document.getElementById('route-count').textContent = num_active_routes;

        // Update the html route list
        let html = '';
        Object.entries(routeCounts).forEach(([route, data]) => {
            html += `
                <div class="route-card">
                    <div class="route-id-col">
                        <div class="route-id" style="background: linear-gradient(rgba(255,255,255,0.4), rgba(255,255,255,0.4)), #${data.color};">
                            ${route}
                        </div>
                    </div>
                    <div class="route-stats-col">
                        <div class="route-detail">
                            ${data.name}
                        </div>
                        <div class="route-detail">
                            ${data.count} vehicles
                        </div>
                    </div>
                </div>
            `
        });
        const routeListContainer = document.querySelector('.route-list');
        routeListContainer.innerHTML = html;
    }).catch(error => console.error("Error fetching data:", error));
    
    
}

// ===== FILTERS =====
function applyRouteFilter() {
    // has access to element with id 'rid' from form
    // trim whitespace and make route IDs case insensitive
    const route_ids_str = document.getElementById('rid').value.trim().toUpperCase();

    // if no routes in filter
    if (!route_ids_str) {
        route_filtered = false;
        route_filtered_list = [];
    } else {
        const new_route_filtered_list = route_ids_str.split(",")
        .map(raw_id => raw_id.trim())
        .filter(route_id => route_id != "");

        if (new_route_filtered_list.length > 0) {
            route_filtered = true;
            route_filtered_list = new_route_filtered_list;
            console.log('Filtering enabled for routes:', route_filtered_list);
        }
    }
    // update map immediately
    updateVehicles();
}

function applyTypeFilter() {
    // has access to element with id 'rid' from form
    // trim whitespace and make route IDs case insensitive
    const route_ids_str = document.getElementById('rid').value.trim().toUpperCase();

    // if no routes in filter
    if (!route_ids_str) {
        type_filtered = false;
        type_filtered_list = [];
    } else {
        const new_type_filtered_list = route_ids_str.split(",")
        .map(raw_id => raw_id.trim())
        .filter(route_id => route_id != "");

        if (new_filtered_list.length > 0) {
            filtered = true;
            type_filtered_list = new_type_filtered_list;
            console.log('Filtering enabled for routes:', type_filtered_list);
        }
    }
    // update map immediately
    updateVehicles();
}

function clearFilters() {
    route_filtered = false;
    route_filtered_list = [];
    document.getElementById('rid').value = '';
    updateVehicles();
}

// ===== POLLING =====
let pollInterval;

document.addEventListener("visibilitychange", () => {
if (document.hidden) {
    clearInterval(pollInterval);
} else {
    updateVehicles();  // optional immediate update
    pollInterval = setInterval(updateVehicles, 30000);
}
});

// Start polling on load
updateVehicles();
pollInterval = setInterval(updateVehicles, 30000);