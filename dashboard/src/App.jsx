import { useEffect, useRef, useState } from "react";
import {
  MapContainer,
  TileLayer,
  CircleMarker,
  Popup,
} from "react-leaflet";

import "leaflet/dist/leaflet.css";
import "./App.css";

const API_BASE_URL = "http://127.0.0.1:8000";

function App() {
  const [metrics, setMetrics] = useState(null);
  const [vehicles, setVehicles] = useState([]);
  const [routes, setRoutes] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastSync, setLastSync] = useState(null);

  const fetchDashboardData = async () => {
    try {
      setError(null);

      const [metricsResponse, vehiclesResponse, routesResponse] =
        await Promise.all([
          fetch(`${API_BASE_URL}/metrics/overview`),
          fetch(`${API_BASE_URL}/vehicles/latest?limit=1000`),
          fetch(`${API_BASE_URL}/routes/activity?limit=20`),
        ]);

      if (
        !metricsResponse.ok ||
        !vehiclesResponse.ok ||
        !routesResponse.ok
      ) {
        throw new Error("Failed to fetch dashboard data");
      }

      const metricsData = await metricsResponse.json();
      const vehiclesData = await vehiclesResponse.json();
      const routesData = await routesResponse.json();

      setMetrics(metricsData);
      setVehicles(vehiclesData.vehicles || []);
      setRoutes(routesData.routes || []);
      setLastSync(new Date());
    } catch (err) {
      console.error(err);
      setError("Unable to connect to the Mobility API.");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchDashboardData();
    const interval = setInterval(fetchDashboardData, 30000);
    return () => clearInterval(interval);
  }, []);

  if (loading) {
    return (
      <div className="app">
        <div className="boot-screen">
          <div className="boot-mark" aria-hidden="true" />
          <p>Connecting to the mobility feed…</p>
        </div>
      </div>
    );
  }

  return (
    <div className="app">
      <header className="ops-bar reveal" style={{ "--d": "0ms" }}>
        <div className="ops-bar__title">
          <span className="ops-bar__glyph" aria-hidden="true">
            ◎
          </span>
          <div>
            <h1>Open City Mobility</h1>
            <p>Delhi · real-time transit intelligence</p>
          </div>
        </div>

        <div className="ops-bar__right">
          {lastSync && (
            <span className="ops-bar__sync">
              synced {formatClock(lastSync)}
            </span>
          )}
          <div className="live-pill">
            <span className="live-dot" />
            Live
          </div>
        </div>
      </header>

      {error && (
        <div className="banner banner--error reveal" style={{ "--d": "60ms" }}>
          <span aria-hidden="true">⚠</span>
          {error}
        </div>
      )}

      <section
        className="metrics-row reveal"
        style={{ "--d": "80ms" }}
      >
        <MetricCard
          label="Active vehicles"
          value={metrics?.total_vehicles ?? 0}
          accent="amber"
        />
        <MetricCard
          label="Active routes"
          value={metrics?.active_routes ?? 0}
          accent="blue"
        />
        <MetricCard
          label="Active trips"
          value={metrics?.active_trips ?? 0}
          accent="green"
        />
      </section>

      <section className="panel reveal" style={{ "--d": "140ms" }}>
        <div className="panel__header">
          <h2>Live vehicle map</h2>
          <span className="panel__meta">{vehicles.length} vehicles</span>
        </div>

        <div className="map-frame">
          <span className="frame-corner frame-corner--tl" />
          <span className="frame-corner frame-corner--tr" />
          <span className="frame-corner frame-corner--bl" />
          <span className="frame-corner frame-corner--br" />

          <MapContainer center={[28.6139, 77.209]} zoom={11} className="map">
            <TileLayer
              attribution="&copy; OpenStreetMap contributors"
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />

            {vehicles.map((vehicle) => {
              if (vehicle.latitude == null || vehicle.longitude == null) {
                return null;
              }

              return (
                <CircleMarker
                  key={`${vehicle.vehicle_id}-${vehicle.kafka_offset}`}
                  center={[vehicle.latitude, vehicle.longitude]}
                  radius={5}
                  pathOptions={{
                    color: "#f2a93b",
                    fillColor: "#f2a93b",
                    fillOpacity: 0.85,
                    weight: 1,
                  }}
                >
                  <Popup>
                    <strong>{vehicle.vehicle_id}</strong>
                    <br />
                    Route: {vehicle.route_id || "N/A"}
                    <br />
                    Trip: {vehicle.trip_id || "N/A"}
                    <br />
                    Latitude: {vehicle.latitude.toFixed(5)}
                    <br />
                    Longitude: {vehicle.longitude.toFixed(5)}
                  </Popup>
                </CircleMarker>
              );
            })}
          </MapContainer>
        </div>
      </section>

      <section className="panel reveal" style={{ "--d": "200ms" }}>
        <div className="panel__header">
          <h2>Route activity</h2>
          <span className="panel__meta">Top 20 routes</span>
        </div>

        <div className="board">
          <div className="board__row board__row--head">
            <span>Route</span>
            <span>Vehicles</span>
            <span>Trips</span>
            <span>Last observed</span>
          </div>

          {routes.map((route) => (
            <div className="board__row" key={route.route_id}>
              <span className="route-chip">{route.route_id}</span>
              <span className="board__num">
                {route.active_vehicle_count}
              </span>
              <span className="board__num">{route.active_trip_count}</span>
              <span className="board__time">
                {formatTimestamp(route.last_observed_at)}
              </span>
            </div>
          ))}

          {routes.length === 0 && (
            <div className="board__empty">
              No route activity reported yet.
            </div>
          )}
        </div>
      </section>
    </div>
  );
}

function MetricCard({ label, value, accent }) {
  const displayValue = useCountUp(value);

  return (
    <div className={`metric-card metric-card--${accent}`}>
      <span className="metric-card__label">{label}</span>
      <strong className="metric-card__value">
        {displayValue.toLocaleString()}
      </strong>
      <span className="metric-card__bar" aria-hidden="true" />
    </div>
  );
}

// Animates a metric from its previous value to the next one whenever it
// changes, rather than snapping — the dashboard is meant to feel alive.
function useCountUp(target) {
  const [display, setDisplay] = useState(target);
  const prevRef = useRef(target);
  const frameRef = useRef(null);

  useEffect(() => {
    const prefersReducedMotion = window.matchMedia(
      "(prefers-reduced-motion: reduce)"
    ).matches;

    if (prefersReducedMotion) {
      setDisplay(target);
      prevRef.current = target;
      return;
    }

    const start = prevRef.current;
    const delta = target - start;
    if (delta === 0) return;

    const duration = 600;
    const startTime = performance.now();

    const tick = (now) => {
      const progress = Math.min((now - startTime) / duration, 1);
      const eased = 1 - Math.pow(1 - progress, 3);
      setDisplay(Math.round(start + delta * eased));

      if (progress < 1) {
        frameRef.current = requestAnimationFrame(tick);
      } else {
        prevRef.current = target;
      }
    };

    frameRef.current = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(frameRef.current);
  }, [target]);

  return display;
}

function formatTimestamp(timestamp) {
  if (!timestamp) return "N/A";
  return new Date(timestamp).toLocaleString();
}

function formatClock(date) {
  return date.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

export default App;