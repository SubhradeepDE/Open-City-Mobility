import { useEffect, useRef, useState } from "react";

import {
  MapContainer,
  TileLayer,
  CircleMarker,
  Popup,
} from "react-leaflet";

import "leaflet/dist/leaflet.css";

const API_BASE_URL = "https://open-city-mobility-api.onrender.com";

function App() {
  const [metrics, setMetrics] = useState(null);
  const [vehicles, setVehicles] = useState([]);
  const [routes, setRoutes] = useState([]);
  const [anomalies, setAnomalies] = useState([]);
  const [alerts, setAlerts] = useState([]);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastSync, setLastSync] = useState(null);

  /*
   * ============================================================
   * FETCH DASHBOARD DATA
   * ============================================================
   */

  const fetchDashboardData = async () => {
    try {
      setError(null);

      const [
        metricsResponse,
        vehiclesResponse,
        routesResponse,
        anomaliesResponse,
        alertsResponse,
      ] = await Promise.all([
        fetch(`${API_BASE_URL}/metrics/overview`),
        fetch(`${API_BASE_URL}/vehicles/live`),
        fetch(`${API_BASE_URL}/routes/activity?limit=20`),
        fetch(`${API_BASE_URL}/analytics/anomalies`),
        fetch(`${API_BASE_URL}/analytics/alerts`),
      ]);

      if (
        !metricsResponse.ok ||
        !vehiclesResponse.ok ||
        !routesResponse.ok ||
        !anomaliesResponse.ok ||
        !alertsResponse.ok
      ) {
        throw new Error("Failed to fetch dashboard data");
      }

      const metricsData = await metricsResponse.json();
      const vehiclesData = await vehiclesResponse.json();
      const routesData = await routesResponse.json();
      const anomaliesData = await anomaliesResponse.json();
      const alertsData = await alertsResponse.json();

      setMetrics(metricsData);
      setVehicles(vehiclesData.vehicles || []);
      setRoutes(routesData.routes || []);
      setAnomalies(anomaliesData.anomalies || []);
      setAlerts(alertsData.alerts || []);

      setLastSync(new Date());
    } catch (err) {
      console.error("Dashboard API error:", err);

      setError("Unable to connect to the Mobility API.");
    } finally {
      setLoading(false);
    }
  };

  /*
   * ============================================================
   * INITIAL FETCH + AUTO REFRESH
   * ============================================================
   */

  useEffect(() => {
    fetchDashboardData();

    const interval = setInterval(
      fetchDashboardData,
      30000
    );

    return () => clearInterval(interval);
  }, []);

  /*
   * ============================================================
   * LOADING SCREEN
   * ============================================================
   */

  if (loading) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-[#081735] px-6">
        <div className="flex flex-col items-center gap-5 text-center">
          <span className="relative flex h-14 w-14 items-center justify-center">
            <span className="absolute h-full w-full animate-ping rounded-full bg-sky-400/30" />
            <span className="relative h-8 w-8 rounded-full bg-gradient-to-br from-sky-300 to-blue-500" />
          </span>

          <p className="font-medium text-slate-300">
            Connecting to the mobility feed&hellip;
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-[#f4f7fc] font-[Inter,sans-serif] text-slate-800">

      {/* ======================================================
          HEADER
      ====================================================== */}

      <header className="sticky top-0 z-[1000] border-b border-white/10 bg-[#0b1e3d] px-5 py-4 shadow-[0_8px_24px_-12px_rgba(11,30,61,0.6)] sm:px-8">
        <div className="mx-auto flex max-w-7xl items-center justify-between gap-4">

          <div className="flex items-center gap-3">
            <span className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-gradient-to-br from-sky-400 to-blue-600 text-lg text-white shadow-inner">
              ◎
            </span>

            <div>
              <h1 className="font-['Space_Grotesk',sans-serif] text-lg font-semibold leading-tight text-white sm:text-xl">
                Open City Mobility
              </h1>

              <p className="text-sm leading-tight text-blue-200/70">
                Delhi transit network, live
              </p>
            </div>
          </div>

          <div className="flex items-center gap-3 sm:gap-4">

            {lastSync && (
              <span className="hidden text-xs text-blue-200/60 sm:block">
                Synced {formatClock(lastSync)}
              </span>
            )}

            <div className="flex items-center gap-2 rounded-full bg-white/10 px-3 py-1.5">
              <span className="relative flex h-2 w-2">
                <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-emerald-400 opacity-75" />
                <span className="relative inline-flex h-2 w-2 rounded-full bg-emerald-400" />
              </span>

              <span className="text-xs font-medium text-white">
                Live
              </span>
            </div>

          </div>
        </div>
      </header>

      <main className="mx-auto max-w-7xl px-5 py-6 sm:px-8 sm:py-8">

        {/* ======================================================
            ERROR BANNER
        ====================================================== */}

        {error && (
          <div className="mb-6 flex items-center gap-3 rounded-2xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
            <span
              aria-hidden="true"
              className="text-base"
            >
              ⚠
            </span>

            {error}
          </div>
        )}

        {/* ======================================================
            METRICS
        ====================================================== */}

        <section className="mb-6 grid grid-cols-1 gap-4 sm:grid-cols-3">

          <MetricCard
            label="Active vehicles"
            value={
              metrics?.total_vehicles ??
              vehicles.length
            }
            accent="amber"
          />

          <MetricCard
            label="Active routes"
            value={
              metrics?.active_routes ??
              routes.length
            }
            accent="blue"
          />

          <MetricCard
            label="Active trips"
            value={
              metrics?.active_trips ?? 0
            }
            accent="green"
          />

        </section>

        {/* ======================================================
            LIVE VEHICLE MAP
        ====================================================== */}

        <section className="mb-6 overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">

          <div className="flex items-center justify-between border-b border-slate-100 px-5 py-4">

            <h2 className="font-['Space_Grotesk',sans-serif] text-base font-semibold text-[#0b1e3d]">
              Live vehicle map
            </h2>

            <span className="text-sm text-slate-500">
              {vehicles.length.toLocaleString()} vehicles
            </span>

          </div>

          <div className="relative h-[420px] w-full sm:h-[520px]">

            <MapContainer
              center={[28.6139, 77.209]}
              zoom={11}
              className="h-full w-full"
            >

              <TileLayer
                attribution="&copy; OpenStreetMap contributors"
                url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
              />

              {vehicles.map((vehicle) => {

                if (
                  vehicle.latitude == null ||
                  vehicle.longitude == null
                ) {
                  return null;
                }

                return (
                  <CircleMarker
                    key={`${vehicle.vehicle_id}-${
                      vehicle.kafka_offset ??
                      vehicle.vehicle_timestamp
                    }`}
                    center={[
                      Number(vehicle.latitude),
                      Number(vehicle.longitude),
                    ]}
                    radius={5}
                    pathOptions={{
                      color: "#0b1e3d",
                      fillColor: "#f2a93b",
                      fillOpacity: 0.9,
                      weight: 1.5,
                    }}
                  >

                    <Popup>

                      <div className="space-y-1 text-sm">

                        <strong className="text-[#0b1e3d]">
                          {vehicle.vehicle_id}
                        </strong>

                        <div>
                          Route:{" "}
                          {vehicle.route_id || "N/A"}
                        </div>

                        <div>
                          Trip:{" "}
                          {vehicle.trip_id || "N/A"}
                        </div>

                        <div>
                          Latitude:{" "}
                          {Number(
                            vehicle.latitude
                          ).toFixed(5)}
                        </div>

                        <div>
                          Longitude:{" "}
                          {Number(
                            vehicle.longitude
                          ).toFixed(5)}
                        </div>

                        <div>
                          Observed:{" "}
                          {formatTimestamp(
                            vehicle.vehicle_timestamp
                          )}
                        </div>

                        <div>
                          Ingested:{" "}
                          {formatTimestamp(
                            vehicle.ingestion_timestamp
                          )}
                        </div>

                      </div>

                    </Popup>

                  </CircleMarker>
                );
              })}

            </MapContainer>

          </div>
        </section>

        {/* ======================================================
            ROUTE ACTIVITY
        ====================================================== */}

        <section className="mb-6 overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">

          <div className="flex items-center justify-between border-b border-slate-100 px-5 py-4">

            <h2 className="font-['Space_Grotesk',sans-serif] text-base font-semibold text-[#0b1e3d]">
              Route activity
            </h2>

            <span className="text-sm text-slate-500">
              Top 20 routes
            </span>

          </div>

          {routes.length === 0 ? (

            <div className="px-5 py-10 text-center text-sm text-slate-500">
              No route activity reported yet.
            </div>

          ) : (

            <div className="overflow-x-auto">

              <table className="w-full min-w-[560px] border-collapse text-left text-sm">

                <thead>

                  <tr className="text-xs text-slate-500">

                    <th className="px-5 py-3 font-medium">
                      Route
                    </th>

                    <th className="px-5 py-3 font-medium">
                      Vehicles
                    </th>

                    <th className="px-5 py-3 font-medium">
                      Trips
                    </th>

                    <th className="px-5 py-3 font-medium">
                      Last observed
                    </th>

                  </tr>

                </thead>

                <tbody>

                  {routes.map((route, i) => (

                    <tr
                      key={route.route_id}
                      className={`border-t border-slate-100 ${
                        i % 2 === 1
                          ? "bg-slate-50/60"
                          : ""
                      }`}
                    >

                      <td className="px-5 py-3">

                        <span className="inline-flex items-center rounded-full bg-blue-50 px-2.5 py-1 text-xs font-semibold text-[#0b1e3d]">
                          {route.route_id}
                        </span>

                      </td>

                      <td className="px-5 py-3 font-medium text-slate-700">
                        {route.active_vehicle_count}
                      </td>

                      <td className="px-5 py-3 font-medium text-slate-700">
                        {route.active_trip_count}
                      </td>

                      <td className="px-5 py-3 text-slate-500">
                        {formatTimestamp(
                          route.last_observed_at
                        )}
                      </td>

                    </tr>

                  ))}

                </tbody>

              </table>

            </div>
          )}

        </section>

        {/* ======================================================
            MOBILITY ANOMALIES
        ====================================================== */}

        <section className="mb-6 overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">

          <div className="flex items-center justify-between border-b border-slate-100 px-5 py-4">

            <div>

              <h2 className="font-['Space_Grotesk',sans-serif] text-base font-semibold text-[#0b1e3d]">
                Mobility anomalies
              </h2>

              <p className="mt-0.5 text-xs text-slate-400">
                Unusual route activity
              </p>

            </div>

            <span
              className={`rounded-full px-2.5 py-1 text-xs font-medium ${
                anomalies.length > 0
                  ? "bg-amber-50 text-amber-700"
                  : "bg-emerald-50 text-emerald-700"
              }`}
            >
              {anomalies.length} detected
            </span>

          </div>

          {anomalies.length === 0 ? (

            <div className="px-5 py-10 text-center text-sm text-slate-500">
              No mobility anomalies detected.
            </div>

          ) : (

            <div className="overflow-x-auto">

              <table className="w-full min-w-[680px] border-collapse text-left text-sm">

                <thead>

                  <tr className="text-xs text-slate-500">

                    <th className="px-5 py-3 font-medium">
                      Route
                    </th>

                    <th className="px-5 py-3 font-medium">
                      Status
                    </th>

                    <th className="px-5 py-3 font-medium">
                      Vehicles
                    </th>

                    <th className="px-5 py-3 font-medium">
                      Average
                    </th>

                    <th className="px-5 py-3 font-medium">
                      Deviation
                    </th>

                  </tr>

                </thead>

                <tbody>

                  {anomalies.map((anomaly, i) => {

                    const isHigh =
                      anomaly.anomaly_status ===
                      "HIGH_ACTIVITY";

                    return (

                      <tr
                        key={`${anomaly.route_id}-${anomaly.hour}`}
                        className={`border-t border-slate-100 ${
                          i % 2 === 1
                            ? "bg-slate-50/60"
                            : ""
                        }`}
                      >

                        <td className="px-5 py-3">

                          <span className="inline-flex items-center rounded-full bg-blue-50 px-2.5 py-1 text-xs font-semibold text-[#0b1e3d]">
                            {anomaly.route_id}
                          </span>

                        </td>

                        <td className="px-5 py-3">

                          <span
                            className={`inline-flex items-center rounded-full px-2.5 py-1 text-xs font-semibold ${
                              isHigh
                                ? "bg-amber-50 text-amber-700"
                                : "bg-blue-50 text-blue-700"
                            }`}
                          >
                            {formatAnomalyStatus(
                              anomaly.anomaly_status
                            )}
                          </span>

                        </td>

                        <td className="px-5 py-3 font-medium text-slate-700">
                          {anomaly.active_vehicles}
                        </td>

                        <td className="px-5 py-3 text-slate-600">
                          {anomaly.avg_active_vehicles != null
                            ? anomaly.avg_active_vehicles.toFixed(1)
                            : "N/A"}
                        </td>

                        <td
                          className={`px-5 py-3 font-semibold ${
                            isHigh
                              ? "text-amber-600"
                              : "text-blue-600"
                          }`}
                        >
                          {anomaly.deviation_percent != null
                            ? `${anomaly.deviation_percent.toFixed(1)}%`
                            : "N/A"}
                        </td>

                      </tr>

                    );
                  })}

                </tbody>

              </table>

            </div>
          )}

        </section>

        {/* ======================================================
            REAL-TIME ALERTS
        ====================================================== */}

        <section className="overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">

          <div className="flex items-center justify-between border-b border-slate-100 px-5 py-4">

            <div>

              <h2 className="font-['Space_Grotesk',sans-serif] text-base font-semibold text-[#0b1e3d]">
                Real-time alerts
              </h2>

              <p className="mt-0.5 text-xs text-slate-400">
                Latest mobility signals
              </p>

            </div>

            <span
              className={`rounded-full px-2.5 py-1 text-xs font-medium ${
                alerts.length > 0
                  ? "bg-red-50 text-red-600"
                  : "bg-emerald-50 text-emerald-700"
              }`}
            >
              {alerts.length} alerts
            </span>

          </div>

          {alerts.length === 0 ? (

            <div className="px-5 py-10 text-center text-sm text-slate-500">
              No active mobility alerts.
            </div>

          ) : (

            <div className="divide-y divide-slate-100">

              {alerts.map((alert) => (

                <div
                  key={alert.alert_id}
                  className="flex flex-col gap-3 px-5 py-4 sm:flex-row sm:items-center sm:justify-between"
                >

                  <div className="flex items-start gap-3">

                    <span
                      className={`mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-lg ${
                        alert.alert_type ===
                        "HIGH_ACTIVITY"
                          ? "bg-amber-50 text-amber-600"
                          : "bg-blue-50 text-blue-600"
                      }`}
                    >
                      ⚠
                    </span>

                    <div>

                      <div className="flex flex-wrap items-center gap-2">

                        <span className="inline-flex items-center rounded-full bg-blue-50 px-2.5 py-1 text-xs font-semibold text-[#0b1e3d]">
                          Route {alert.route_id}
                        </span>

                        <span className="text-xs font-medium text-slate-500">
                          {formatAlertType(
                            alert.alert_type
                          )}
                        </span>

                      </div>

                      <p className="mt-1 text-sm text-slate-600">
                        {alert.message}
                      </p>

                    </div>

                  </div>

                  <div className="flex shrink-0 items-center gap-3 sm:flex-col sm:items-end">

                    {alert.deviation_percent != null && (
                      <span className="text-sm font-semibold text-[#0b1e3d]">
                        {alert.deviation_percent.toFixed(1)}%
                      </span>
                    )}

                    <span className="text-xs text-slate-400">
                      {formatTimestamp(
                        alert.alert_time
                      )}
                    </span>

                  </div>

                </div>

              ))}

            </div>
          )}

        </section>

      </main>
    </div>
  );
}

/* ============================================================
   METRIC CARD
============================================================ */

const ACCENTS = {
  amber: {
    bar: "bg-amber-400",
  },
  blue: {
    bar: "bg-blue-500",
  },
  green: {
    bar: "bg-emerald-400",
  },
};

function MetricCard({
  label,
  value,
  accent,
}) {
  const displayValue = useCountUp(
    Number(value) || 0
  );

  const tone =
    ACCENTS[accent] || ACCENTS.blue;

  return (
    <div className="relative overflow-hidden rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">

      <span
        className={`absolute left-0 top-0 h-full w-1 ${tone.bar}`}
      />

      <span className="block text-sm text-slate-500">
        {label}
      </span>

      <strong className="mt-1 block font-['Space_Grotesk',sans-serif] text-3xl font-semibold text-[#0b1e3d]">
        {displayValue.toLocaleString()}
      </strong>

    </div>
  );
}

/* ============================================================
   COUNT-UP ANIMATION
============================================================ */

function useCountUp(target) {
  const [display, setDisplay] =
    useState(target);

  const prevRef = useRef(target);
  const frameRef = useRef(null);

  useEffect(() => {
    const prefersReducedMotion =
      window
        .matchMedia(
          "(prefers-reduced-motion: reduce)"
        )
        .matches;

    if (prefersReducedMotion) {
      setDisplay(target);
      prevRef.current = target;
      return;
    }

    const start = prevRef.current;
    const delta = target - start;

    if (delta === 0) {
      return;
    }

    const duration = 600;
    const startTime =
      performance.now();

    const tick = (now) => {
      const progress = Math.min(
        (now - startTime) / duration,
        1
      );

      const eased =
        1 - Math.pow(1 - progress, 3);

      setDisplay(
        Math.round(
          start + delta * eased
        )
      );

      if (progress < 1) {
        frameRef.current =
          requestAnimationFrame(tick);
      } else {
        prevRef.current = target;
      }
    };

    frameRef.current =
      requestAnimationFrame(tick);

    return () => {
      if (frameRef.current) {
        cancelAnimationFrame(
          frameRef.current
        );
      }
    };
  }, [target]);

  return display;
}

/* ============================================================
   FORMAT TIMESTAMP
============================================================ */

function formatTimestamp(timestamp) {
  if (!timestamp) {
    return "N/A";
  }

  const date = new Date(timestamp);

  if (Number.isNaN(date.getTime())) {
    return "N/A";
  }

  return date.toLocaleString();
}

/* ============================================================
   FORMAT CLOCK
============================================================ */

function formatClock(date) {
  return date.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

/* ============================================================
   FORMAT ANOMALY STATUS
============================================================ */

function formatAnomalyStatus(status) {
  if (!status) {
    return "UNKNOWN";
  }

  return status
    .replaceAll("_", " ")
    .toLowerCase()
    .replace(/\b\w/g, (char) =>
      char.toUpperCase()
    );
}

/* ============================================================
   FORMAT ALERT TYPE
============================================================ */

function formatAlertType(type) {
  if (!type) {
    return "Alert";
  }

  return type
    .replaceAll("_", " ")
    .toLowerCase()
    .replace(/\b\w/g, (char) =>
      char.toUpperCase()
    );
}

export default App;