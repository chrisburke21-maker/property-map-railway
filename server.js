const dns = require('dns');
dns.setDefaultResultOrder('ipv4first');
const http = require('http');
const { Client } = require('pg');

const dbConfig = {
  host: process.env.DATABASE_HOST || 'aws-0-us-west-2.pooler.supabase.com',
  port: parseInt(process.env.DATABASE_PORT || '5432'),
  database: process.env.DATABASE_NAME || 'postgres',
  user: process.env.DATABASE_USER || 'postgres.xanfkjitudcpduodadgu',
  password: process.env.DATABASE_PASSWORD,
  ssl: { rejectUnauthorized: false },
  connectionTimeoutMillis: 20000
};

const SQL = `
SELECT p.id, p.apn, p.county, p.state, p.acreage,
       p.acreage_range::text AS acreage_range,
       p.current_status::text AS status,
       p.county_identifier, p.subdivision,
       p.gps_lat, p.gps_long,
       p.date_first_seen, p.date_last_seen,
       (CURRENT_DATE - p.date_first_seen) AS days_on_market,
       COALESCE(json_agg(json_build_object(
         'platform', l.source_platform,
         'url', l.listing_url,
         'price', l.cash_price,
         'seller', l.seller_name,
         'terms_down', l.terms_down_payment,
         'terms_monthly', l.terms_monthly_payment,
         'terms_months', l.terms_length_months
       )) FILTER (WHERE l.id IS NOT NULL), '[]') AS listings
FROM properties p
LEFT JOIN listings l ON l.property_id = p.id AND l.listing_status = 'active'
WHERE p.gps_lat IS NOT NULL AND p.gps_long IS NOT NULL
GROUP BY p.id
ORDER BY p.county, p.apn`;

const PRICING_MATRIX_SQL = `
SELECT pm.id, pm.apn_raw, pm.apn_normalized, pm.county, pm.state,
       pm.county_identifier, pm.subdivision,
       pm.acreage, pm.gps_lat, pm.gps_long,
       pm.cash_price, pm.terms_down_payment, pm.terms_monthly_payment,
       pm.terms_length_months, pm.source_platform, pm.listing_url,
       pm.date_observed
FROM pricing_matrix pm
WHERE pm.gps_lat IS NOT NULL AND pm.gps_long IS NOT NULL
  AND pm.property_id IS NULL
ORDER BY pm.county, pm.apn_normalized`;

const PRICING_ENRICHMENT_SQL = `
SELECT pm.property_id,
       json_agg(json_build_object(
         'platform', pm.source_platform,
         'url', pm.listing_url,
         'cash_price', pm.cash_price,
         'down', pm.terms_down_payment,
         'monthly', pm.terms_monthly_payment,
         'months', pm.terms_length_months,
         'date', pm.date_observed
       ) ORDER BY pm.date_observed DESC) AS airtable_prices
FROM pricing_matrix pm
WHERE pm.property_id IS NOT NULL
GROUP BY pm.property_id`;

async function fetchProperties() {
  const client = new Client(dbConfig);
  await client.connect();
  const [propResult, enrichResult] = await Promise.all([
    client.query(SQL),
    client.query(PRICING_ENRICHMENT_SQL)
  ]);
  await client.end();

  const enrichMap = {};
  for (const row of enrichResult.rows) {
    enrichMap[row.property_id] = row.airtable_prices;
  }

  return propResult.rows.map(r => ({
    id: r.id,
    apn: r.apn,
    county: r.county,
    state: r.state,
    acreage: parseFloat(r.acreage),
    acreage_range: r.acreage_range,
    status: r.status,
    county_identifier: r.county_identifier,
    subdivision: r.subdivision,
    gps_lat: parseFloat(r.gps_lat),
    gps_long: parseFloat(r.gps_long),
    dom: r.days_on_market,
    listings: r.listings || [],
    airtable_prices: enrichMap[r.id] || []
  }));
}

async function fetchPricingMatrix() {
  const client = new Client(dbConfig);
  await client.connect();
  const result = await client.query(PRICING_MATRIX_SQL);
  await client.end();
  return result.rows.map(r => ({
    id: r.id,
    apn: r.apn_normalized || r.apn_raw,
    county: r.county,
    state: r.state,
    county_identifier: r.county_identifier,
    subdivision: r.subdivision,
    acreage: r.acreage ? parseFloat(r.acreage) : null,
    gps_lat: parseFloat(r.gps_lat),
    gps_long: parseFloat(r.gps_long),
    cash_price: r.cash_price ? parseFloat(r.cash_price) : null,
    down: r.terms_down_payment ? parseFloat(r.terms_down_payment) : null,
    monthly: r.terms_monthly_payment ? parseFloat(r.terms_monthly_payment) : null,
    months: r.terms_length_months,
    platform: r.source_platform,
    url: r.listing_url,
    date: r.date_observed
  }));
}

function buildHTML(properties, pricingMatrix) {
  const generated = new Date().toISOString();
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Simpli Acres — Property Map</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"><\/script>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
  #controls {
    display: flex; flex-wrap: wrap; gap: 12px; align-items: flex-end;
    padding: 12px 16px; background: #1e293b; color: #f1f5f9;
  }
  #controls .field { display: flex; flex-direction: column; gap: 4px; }
  #controls label { font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; color: #94a3b8; }
  #controls input, #controls select {
    padding: 6px 10px; border: 1px solid #475569; border-radius: 4px;
    background: #334155; color: #f1f5f9; font-size: 14px; width: 140px;
  }
  #controls select { width: 150px; }
  #controls button {
    padding: 6px 18px; background: #3b82f6; color: white; border: none;
    border-radius: 4px; font-size: 14px; cursor: pointer; height: 33px;
  }
  #controls button:hover { background: #2563eb; }
  #controls .count { font-size: 13px; color: #94a3b8; align-self: center; margin-left: auto; }
  #map { height: calc(100vh - 56px); width: 100%; }
  .legend {
    background: white; padding: 10px 14px; border-radius: 6px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.2); font-size: 12px; line-height: 1.8;
  }
  .legend h4 { margin: 0 0 6px; font-size: 13px; }
  .legend-item { display: flex; align-items: center; gap: 6px; }
  .legend-dot {
    width: 12px; height: 12px; border-radius: 50%; display: inline-block;
    border: 1px solid rgba(0,0,0,0.2);
  }
  .leaflet-popup-content { font-size: 13px; line-height: 1.6; min-width: 300px; max-width: 400px; }
  .leaflet-popup-content h3 { font-size: 16px; margin: 0 0 4px; color: #1e293b; }
  .leaflet-popup-content .sub { color: #64748b; font-size: 12px; margin-bottom: 8px; }
  .leaflet-popup-content .detail { color: #475569; }
  .leaflet-popup-content a { color: #3b82f6; text-decoration: none; }
  .leaflet-popup-content a:hover { text-decoration: underline; }
  .leaflet-popup-content .price { font-weight: 700; color: #16a34a; font-size: 16px; }
  .leaflet-popup-content .section { margin-top: 8px; padding-top: 8px; border-top: 1px solid #e2e8f0; }
  .leaflet-popup-content .listing-row { margin: 4px 0; padding: 4px 0; }
  .leaflet-popup-content .terms { color: #6366f1; font-size: 12px; }
  .leaflet-tooltip { font-size: 12px; padding: 6px 10px; }
  #radiusStats {
    display: none; padding: 8px 16px; background: #0f172a; color: #f1f5f9;
    border-top: 1px solid #334155; gap: 24px; align-items: center; flex-wrap: wrap;
  }
  #radiusStats.active { display: flex; }
  .stat-box { text-align: center; }
  .stat-box .stat-value { font-size: 18px; font-weight: 700; color: #22c55e; }
  .stat-box .stat-label { font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; color: #94a3b8; }
  .stat-box .stat-na { font-size: 18px; font-weight: 700; color: #475569; }
</style>
</head>
<body>

<div id="controls">
  <div class="field">
    <label>Min Acres</label>
    <input type="number" id="minAcres" placeholder="0" step="any">
  </div>
  <div class="field">
    <label>Max Acres</label>
    <input type="number" id="maxAcres" placeholder="Any" step="any">
  </div>
  <div class="field">
    <label>County ID</label>
    <input type="text" id="countyId" placeholder="Search...">
  </div>
  <div class="field">
    <label>Status</label>
    <select id="status">
      <option value="">All</option>
      <option value="active">Active</option>
      <option value="assumed_sold">Assumed Sold</option>
    </select>
  </div>
  <div class="field">
    <label>Radius (mi)</label>
    <input type="number" id="radius" placeholder="Off" step="any" style="width:90px">
  </div>
  <label style="display:flex;align-items:center;gap:6px;cursor:pointer;font-size:12px;color:#d946ef;margin-left:4px">
    <input type="checkbox" id="showPM" checked onchange="togglePricingLayer(this.checked)" style="accent-color:#d946ef;width:16px;height:16px;cursor:pointer">
    Airtable
  </label>
  <button onclick="applyFilters()">Apply</button>
  <button onclick="resetFilters()" style="background:#475569">Reset</button>
  <button onclick="clearRadius()" id="clearRadiusBtn" style="background:#dc2626;display:none">Clear Radius</button>
  <button onclick="toggleDrawMode()" id="drawPolyBtn" style="background:#8b5cf6">Draw Area</button>
  <button onclick="closePolygon()" id="closePolyBtn" style="background:#22c55e;display:none">Close Shape</button>
  <button onclick="clearPolygon()" id="clearPolyBtn" style="background:#dc2626;display:none">Clear Area</button>
  <span class="count" id="pinCount"></span>
  <span id="radiusInfo" style="font-size:12px;color:#fbbf24;margin-left:8px"></span>
  <span id="polyInfo" style="font-size:12px;color:#c4b5fd;margin-left:8px"></span>
  <button onclick="refreshData()" id="refreshBtn" style="background:#475569;margin-left:auto">Refresh Data</button>
</div>

<div id="radiusStats">
  <div class="stat-box"><div class="stat-value" id="statCashAc">—</div><div class="stat-label">Avg Cash / Acre</div></div>
  <div class="stat-box"><div class="stat-value" id="statDownAc">—</div><div class="stat-label">Avg Down / Acre</div></div>
  <div class="stat-box"><div class="stat-value" id="statMonthlyAc">—</div><div class="stat-label">Avg Monthly / Acre</div></div>
  <div class="stat-box"><div class="stat-value" id="statTerm">—</div><div class="stat-label">Avg Term (mo)</div></div>
  <div style="border-left:1px solid #334155;height:36px;margin:0 4px"></div>
  <div class="stat-box"><div class="stat-value" id="statCash">—</div><div class="stat-label">Avg Cash</div></div>
  <div class="stat-box"><div class="stat-value" id="statDown">—</div><div class="stat-label">Avg Down</div></div>
  <div class="stat-box"><div class="stat-value" id="statMonthly">—</div><div class="stat-label">Avg Monthly</div></div>
  <div class="stat-box"><div class="stat-value" id="statTermOverall">—</div><div class="stat-label">Avg Term</div></div>
  <div style="border-left:1px solid #334155;height:36px;margin:0 4px"></div>
  <div class="stat-box"><div class="stat-value" id="statPmCashAc" style="color:#e879f9">—</div><div class="stat-label">AT Cash/Ac</div></div>
  <div class="stat-box"><div class="stat-value" id="statPmDownAc" style="color:#e879f9">—</div><div class="stat-label">AT Down/Ac</div></div>
  <div class="stat-box"><div class="stat-value" id="statPmMonthlyAc" style="color:#e879f9">—</div><div class="stat-label">AT Monthly/Ac</div></div>
  <div class="stat-box"><div class="stat-value" id="statPmCount" style="color:#e879f9">—</div><div class="stat-label">AT Entries</div></div>
</div>

<div id="map"></div>

<script>
// Data generated ${generated}
window.PROPERTIES = ${JSON.stringify(properties)};
window.PRICING_MATRIX = ${JSON.stringify(pricingMatrix)};

const COLORS = {
  'under_1':  '#3B82F6',
  '1_to_2':   '#22C55E',
  '2_to_5':   '#10B981',
  '5_to_10':  '#EAB308',
  '10_to_20': '#F97316',
  '20_plus':  '#EF4444'
};
const DEFAULT_COLOR = '#6B7280';

const map = L.map('map', { doubleClickZoom: false }).setView([34.5, -111.9], 7);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '&copy; OpenStreetMap contributors',
  maxZoom: 19
}).addTo(map);

// Legend
const legend = L.control({ position: 'bottomright' });
legend.onAdd = function() {
  const div = L.DomUtil.create('div', 'legend');
const LABELS = {
    'under_1': '< 1 acre',
    '1_to_2': '1–2 acres',
    '2_to_5': '2–5 acres',
    '5_to_10': '5–10 acres',
    '10_to_20': '10–20 acres',
    '20_plus': '20+ acres'
  };
  div.innerHTML = '<h4>Acreage Range</h4>' +
    Object.entries(COLORS).map(([range, color]) =>
      '<div class="legend-item"><span class="legend-dot" style="background:' + color + '"></span>' + (LABELS[range] || range) + '</div>'
    ).join('') +
    '<div style="margin-top:8px;border-top:1px solid #e2e8f0;padding-top:6px">' +
    '<h4>Data Source</h4>' +
    '<div class="legend-item"><span style="width:12px;height:12px;background:#d946ef;border:2px solid #86198f;border-radius:50%;display:inline-block"></span> Airtable (Unmatched)</div>' +
    '</div>';
  return div;
};
legend.addTo(map);

// Pricing markers use a high-zIndex pane with DOM-based markers for reliable clicks
map.createPane('pricingPane');
map.getPane('pricingPane').style.zIndex = 650;

let markers = L.layerGroup().addTo(map);
let pricingMarkers = L.layerGroup().addTo(map);

function togglePricingLayer(show) {
  if (show) { map.addLayer(pricingMarkers); }
  else { map.removeLayer(pricingMarkers); }
}

// Radius filter state
let radiusCenter = null;  // { lat, lng, apn }
let radiusCircle = null;   // L.circle instance

function haversine(lat1, lon1, lat2, lon2) {
  const R = 3958.8; // Earth radius in miles
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
    Math.sin(dLon/2) * Math.sin(dLon/2);
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function computeRadiusStats(filtered) {
  let cashPerAcre = [], downPerAcre = [], monthlyPerAcre = [], termMonths = [];
  let cashTotal = [], downTotal = [], monthlyTotal = [];
  filtered.forEach(p => {
    const listings = p.listings || [];
    // Cash: use lowest price listing
    const cashListings = listings.filter(l => l.price > 0).sort((a,b) => a.price - b.price);
    if (cashListings.length) {
      cashTotal.push(cashListings[0].price);
      if (p.acreage && p.acreage > 0) cashPerAcre.push(cashListings[0].price / p.acreage);
    }
    // Terms: use first listing with terms data
    const termsListing = listings.find(l => l.terms_down || l.terms_monthly);
    if (termsListing) {
      if (termsListing.terms_down) {
        downTotal.push(termsListing.terms_down);
        if (p.acreage && p.acreage > 0) downPerAcre.push(termsListing.terms_down / p.acreage);
      }
      if (termsListing.terms_monthly) {
        monthlyTotal.push(termsListing.terms_monthly);
        if (p.acreage && p.acreage > 0) monthlyPerAcre.push(termsListing.terms_monthly / p.acreage);
      }
      if (termsListing.terms_months) termMonths.push(termsListing.terms_months);
    }
  });
  const avg = arr => arr.length ? arr.reduce((a,b) => a+b, 0) / arr.length : null;
  return {
    cashPerAcre: avg(cashPerAcre), downPerAcre: avg(downPerAcre), monthlyPerAcre: avg(monthlyPerAcre), termMonths: avg(termMonths),
    cashTotal: avg(cashTotal), downTotal: avg(downTotal), monthlyTotal: avg(monthlyTotal)
  };
}

function renderPricingMarkers(data) {
  pricingMarkers.clearLayers();
  data.forEach(function(pm) {
    var tip = '<b>' + (pm.apn || 'No APN') + '</b>';
    if (pm.acreage) tip += ' — ' + pm.acreage + ' ac';
    if (pm.cash_price) tip += ' — ' + formatPrice(pm.cash_price);
    tip += '<br><span style="color:#e879f9">Airtable / ' + (pm.platform || 'Unknown') + '</span>';

    var popup = '<h3 style="color:#a21caf">' + (pm.apn || 'No APN') + '</h3>';
    popup += '<div class="sub">' + pm.county + ', ' + pm.state + ' &bull; <span style="color:#e879f9">Pricing Matrix</span></div>';
    popup += '<table style="width:100%;font-size:13px;border-collapse:collapse">';
    if (pm.acreage) popup += '<tr><td style="color:#64748b">Acreage</td><td style="text-align:right"><b>' + pm.acreage + '</b> ac</td></tr>';
    if (pm.cash_price) popup += '<tr><td style="color:#64748b">Cash Price</td><td style="text-align:right"><span style="font-weight:700;color:#a21caf">' + formatPrice(pm.cash_price) + '</span></td></tr>';
    if (pm.cash_price && pm.acreage) popup += '<tr><td style="color:#64748b">Cash $/Acre</td><td style="text-align:right">' + formatPrice(pm.cash_price / pm.acreage) + '</td></tr>';
    if (pm.down) popup += '<tr><td style="color:#64748b">Down Payment</td><td style="text-align:right">' + formatPrice(pm.down) + '</td></tr>';
    if (pm.monthly) popup += '<tr><td style="color:#64748b">Monthly</td><td style="text-align:right">' + formatPrice(pm.monthly) + '/mo</td></tr>';
    if (pm.months) popup += '<tr><td style="color:#64748b">Term</td><td style="text-align:right">' + pm.months + ' months</td></tr>';
    if (pm.platform) popup += '<tr><td style="color:#64748b">Platform</td><td style="text-align:right">' + pm.platform + '</td></tr>';
    if (pm.date) popup += '<tr><td style="color:#64748b">Date Observed</td><td style="text-align:right">' + pm.date + '</td></tr>';
    popup += '</table>';
    if (pm.url) popup += '<div style="margin-top:6px"><a href="' + pm.url + '" target="_blank">View Listing</a></div>';

    var icon = L.divIcon({
      className: '',
      html: '<div style="width:14px;height:14px;background:#d946ef;border:2px solid #86198f;border-radius:50%;cursor:pointer"></div>',
      iconSize: [14, 14],
      iconAnchor: [7, 7]
    });
    var marker = L.marker([pm.gps_lat, pm.gps_long], { icon: icon, pane: 'pricingPane' })
      .bindTooltip(tip, { direction: 'top', offset: [0, -10] })
      .bindPopup(popup, { maxWidth: 400 });
    pricingMarkers.addLayer(marker);
  });
}

function computePMStats(filtered) {
  var cashPerAcre = [], downPerAcre = [], monthlyPerAcre = [];
  filtered.forEach(function(pm) {
    if (pm.cash_price && pm.cash_price > 0 && pm.acreage && pm.acreage > 0) {
      cashPerAcre.push(pm.cash_price / pm.acreage);
    }
    if (pm.down && pm.down > 0 && pm.acreage && pm.acreage > 0) {
      downPerAcre.push(pm.down / pm.acreage);
    }
    if (pm.monthly && pm.monthly > 0 && pm.acreage && pm.acreage > 0) {
      monthlyPerAcre.push(pm.monthly / pm.acreage);
    }
  });
  var avg = function(arr) { return arr.length ? arr.reduce(function(a,b){return a+b;}, 0) / arr.length : null; };
  return { cashPerAcre: avg(cashPerAcre), downPerAcre: avg(downPerAcre), monthlyPerAcre: avg(monthlyPerAcre), count: filtered.length };
}

function showRadiusStats(filtered, filteredPM) {
  const stats = computeRadiusStats(filtered);
  const fmt = v => v != null ? formatPrice(v) : null;
  document.getElementById('statCashAc').textContent = fmt(stats.cashPerAcre) || 'N/A';
  document.getElementById('statCashAc').className = stats.cashPerAcre != null ? 'stat-value' : 'stat-na';
  document.getElementById('statDownAc').textContent = fmt(stats.downPerAcre) || 'N/A';
  document.getElementById('statDownAc').className = stats.downPerAcre != null ? 'stat-value' : 'stat-na';
  document.getElementById('statMonthlyAc').textContent = fmt(stats.monthlyPerAcre) || 'N/A';
  document.getElementById('statMonthlyAc').className = stats.monthlyPerAcre != null ? 'stat-value' : 'stat-na';
  document.getElementById('statTerm').textContent = stats.termMonths != null ? Math.round(stats.termMonths) + ' mo' : 'N/A';
  document.getElementById('statTerm').className = stats.termMonths != null ? 'stat-value' : 'stat-na';
  // Overall averages
  document.getElementById('statCash').textContent = fmt(stats.cashTotal) || 'N/A';
  document.getElementById('statCash').className = stats.cashTotal != null ? 'stat-value' : 'stat-na';
  document.getElementById('statDown').textContent = fmt(stats.downTotal) || 'N/A';
  document.getElementById('statDown').className = stats.downTotal != null ? 'stat-value' : 'stat-na';
  document.getElementById('statMonthly').textContent = fmt(stats.monthlyTotal) || 'N/A';
  document.getElementById('statMonthly').className = stats.monthlyTotal != null ? 'stat-value' : 'stat-na';
  document.getElementById('statTermOverall').textContent = stats.termMonths != null ? Math.round(stats.termMonths) + ' mo' : 'N/A';
  document.getElementById('statTermOverall').className = stats.termMonths != null ? 'stat-value' : 'stat-na';
  // Airtable pricing matrix stats
  const pmStats = computePMStats(filteredPM || []);
  document.getElementById('statPmCashAc').textContent = fmt(pmStats.cashPerAcre) || 'N/A';
  document.getElementById('statPmDownAc').textContent = fmt(pmStats.downPerAcre) || 'N/A';
  document.getElementById('statPmMonthlyAc').textContent = fmt(pmStats.monthlyPerAcre) || 'N/A';
  document.getElementById('statPmCount').textContent = pmStats.count || '0';
  document.getElementById('radiusStats').classList.add('active');
}

function hideRadiusStats() {
  document.getElementById('radiusStats').classList.remove('active');
}

// Polygon drawing state
let drawMode = false;
let polyPoints = [];
let polyVertexMarkers = [];
let polyLines = null;
let polyShape = null;

function pointInPolygon(lat, lng, polygon) {
  let inside = false;
  for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
    const yi = polygon[i][0], xi = polygon[i][1];
    const yj = polygon[j][0], xj = polygon[j][1];
    if (((yi > lat) !== (yj > lat)) && (lng < (xj - xi) * (lat - yi) / (yj - yi) + xi)) {
      inside = !inside;
    }
  }
  return inside;
}

function toggleDrawMode() {
  if (drawMode) {
    // Cancel drawing
    drawMode = false;
    clearPartialDraw();
    document.getElementById('drawPolyBtn').textContent = 'Draw Area';
    document.getElementById('drawPolyBtn').style.background = '#8b5cf6';
    document.getElementById('closePolyBtn').style.display = 'none';
    document.getElementById('polyInfo').textContent = '';
    map.getContainer().style.cursor = '';
  } else {
    // Enter draw mode
    drawMode = true;
    // Clear existing polygon if any
    if (polyShape) clearPolygon();
    polyPoints = [];
    document.getElementById('drawPolyBtn').textContent = 'Cancel Draw';
    document.getElementById('drawPolyBtn').style.background = '#dc2626';
    document.getElementById('polyInfo').textContent = 'Click map to place points.';
    map.getContainer().style.cursor = 'crosshair';
  }
}

function clearPartialDraw() {
  polyVertexMarkers.forEach(m => map.removeLayer(m));
  polyVertexMarkers = [];
  if (polyLines) { map.removeLayer(polyLines); polyLines = null; }
  polyPoints = [];
}

function closePolygon() {
  if (polyPoints.length < 3) return;
  drawMode = false;
  // Remove vertex markers and preview line
  polyVertexMarkers.forEach(m => map.removeLayer(m));
  polyVertexMarkers = [];
  if (polyLines) { map.removeLayer(polyLines); polyLines = null; }
  // Create the polygon shape
  polyShape = L.polygon(polyPoints, {
    color: '#8b5cf6', weight: 2, fillColor: '#8b5cf6', fillOpacity: 0.08,
    dashArray: '6,4'
  }).addTo(map);
  // Update UI
  document.getElementById('drawPolyBtn').textContent = 'Draw Area';
  document.getElementById('drawPolyBtn').style.background = '#8b5cf6';
  document.getElementById('closePolyBtn').style.display = 'none';
  document.getElementById('clearPolyBtn').style.display = '';
  map.getContainer().style.cursor = '';
  // Filter
  applyFilters();
}

function clearPolygon() {
  if (polyShape) { map.removeLayer(polyShape); polyShape = null; }
  clearPartialDraw();
  polyPoints = [];
  document.getElementById('clearPolyBtn').style.display = 'none';
  document.getElementById('polyInfo').textContent = '';
  applyFilters();
}

// Map click handler for polygon drawing — use DOM event on map container for reliability
map.getContainer().addEventListener('click', function(e) {
  if (!drawMode) return;
  // Convert pixel click to lat/lng
  const rect = map.getContainer().getBoundingClientRect();
  const point = L.point(e.clientX - rect.left, e.clientY - rect.top);
  const latlng = map.containerPointToLatLng(point);
  const pt = [latlng.lat, latlng.lng];

  // If clicking near the first vertex and we have 3+ points, close the polygon
  if (polyPoints.length >= 3) {
    const first = map.latLngToContainerPoint(polyPoints[0]);
    const clicked = L.point(e.clientX - rect.left, e.clientY - rect.top);
    if (first.distanceTo(clicked) < 15) {
      closePolygon();
      return;
    }
  }

  polyPoints.push(pt);
  // Add vertex marker
  const vm = L.circleMarker(pt, {
    radius: 6, fillColor: '#8b5cf6', color: '#fff', weight: 2, fillOpacity: 1
  }).addTo(map);
  polyVertexMarkers.push(vm);
  // Update preview polyline
  if (polyLines) map.removeLayer(polyLines);
  polyLines = L.polyline(polyPoints, { color: '#8b5cf6', weight: 2, dashArray: '4,4' }).addTo(map);
  // Update info + show Close Shape button after 3 points
  if (polyPoints.length < 3) {
    document.getElementById('polyInfo').textContent = polyPoints.length + ' points — need ' + (3 - polyPoints.length) + ' more';
    document.getElementById('closePolyBtn').style.display = 'none';
  } else {
    document.getElementById('polyInfo').textContent = polyPoints.length + ' points — click 1st point or Close Shape';
    document.getElementById('closePolyBtn').style.display = '';
  }
});

function formatPrice(p) {
  if (!p && p !== 0) return 'N/A';
  return '$' + Number(p).toLocaleString('en-US', { maximumFractionDigits: 0 });
}

function renderMarkers(data) {
  markers.clearLayers();
  data.forEach(p => {
    const color = COLORS[p.acreage_range] || DEFAULT_COLOR;
    const listings = p.listings || [];
    const lowestCash = listings.filter(l => l.price).sort((a,b) => a.price - b.price)[0];
    const withTerms = listings.find(l => l.terms_down || l.terms_monthly);

    // Hover tooltip — APN, size, cash price, terms
    let tip = '<b>' + p.apn + '</b> — ' + p.acreage + ' ac';
    if (lowestCash) tip += ' — ' + formatPrice(lowestCash.price);
    // Airtable pricing in tooltip
    const atp = p.airtable_prices || [];
    if (atp.length) {
      const bestAT = atp.find(a => a.cash_price && a.cash_price > 0);
      if (bestAT) tip += '<br><span style="color:#e879f9">AT: ' + formatPrice(bestAT.cash_price) + '</span>';
    }
    if (withTerms) {
      tip += '<br><span style="color:#6366f1">';
      if (withTerms.terms_down) tip += formatPrice(withTerms.terms_down) + ' down';
      if (withTerms.terms_monthly) tip += ' / ' + formatPrice(withTerms.terms_monthly) + '/mo';
      if (withTerms.terms_months) tip += ' x ' + withTerms.terms_months + ' mo';
      tip += '</span>';
    }

    // Click popup — full drill-down
    let popup = '<h3>' + p.apn + '</h3>';
    popup += '<div class="sub">' + p.county + ', ' + p.state;
    if (p.county_identifier) popup += ' &bull; ID: ' + p.county_identifier;
    popup += '</div>';
    popup += '<table style="width:100%;font-size:13px;border-collapse:collapse">';
    popup += '<tr><td style="color:#64748b">Acreage</td><td style="text-align:right"><b>' + p.acreage + '</b> ac</td></tr>';
    popup += '<tr><td style="color:#64748b">Status</td><td style="text-align:right">' + p.status + '</td></tr>';
    if (p.subdivision) popup += '<tr><td style="color:#64748b">Subdivision</td><td style="text-align:right">' + p.subdivision + '</td></tr>';
    if (p.dom != null) popup += '<tr><td style="color:#64748b">Days on Market</td><td style="text-align:right">' + p.dom + '</td></tr>';
    popup += '</table>';

    if (listings.length) {
      popup += '<div class="section"><b>Listings (' + listings.length + ')</b></div>';
      listings.forEach(l => {
        popup += '<div class="listing-row">';
        popup += '<a href="' + l.url + '" target="_blank">' + l.platform + '</a>';
        if (l.seller) popup += ' <span style="color:#94a3b8">(' + l.seller + ')</span>';
        popup += '<br>';
        if (l.price) popup += '<span class="price">' + formatPrice(l.price) + '</span> cash';
        if (l.terms_down || l.terms_monthly) {
          popup += '<br><span class="terms">';
          if (l.terms_down) popup += formatPrice(l.terms_down) + ' down';
          if (l.terms_monthly) popup += ' / ' + formatPrice(l.terms_monthly) + '/mo';
          if (l.terms_months) popup += ' x ' + l.terms_months + ' mo';
          popup += '</span>';
        }
        popup += '</div>';
      });
    } else {
      popup += '<div class="section detail">No active listings</div>';
    }

    // Airtable pricing enrichment
    if (atp.length) {
      popup += '<div class="section"><b style="color:#a21caf">Airtable Pricing (' + atp.length + ')</b></div>';
      atp.forEach(function(a) {
        popup += '<div class="listing-row">';
        if (a.url) popup += '<a href="' + a.url + '" target="_blank" style="color:#a21caf">' + (a.platform || 'Link') + '</a>';
        else if (a.platform) popup += '<span style="color:#a21caf">' + a.platform + '</span>';
        if (a.seller) popup += ' <span style="color:#94a3b8">(' + a.seller + ')</span>';
        if (a.date) popup += ' <span style="color:#94a3b8">' + a.date.substring(0, 10) + '</span>';
        popup += '<br>';
        if (a.cash_price) popup += '<span style="font-weight:700;color:#a21caf">' + formatPrice(a.cash_price) + '</span> cash';
        if (a.down || a.monthly) {
          popup += '<br><span style="color:#c084fc;font-size:12px">';
          if (a.down) popup += formatPrice(a.down) + ' down';
          if (a.monthly) popup += ' / ' + formatPrice(a.monthly) + '/mo';
          if (a.months) popup += ' x ' + a.months + ' mo';
          popup += '</span>';
        }
        popup += '</div>';
      });
    }

    // Highlight the center pin if radius is active
    const isCenter = radiusCenter && p.gps_lat === radiusCenter.lat && p.gps_long === radiusCenter.lng;
    const marker = L.circleMarker([p.gps_lat, p.gps_long], {
      radius: isCenter ? 10 : 7,
      fillColor: isCenter ? '#fbbf24' : color,
      color: isCenter ? '#b45309' : '#fff',
      weight: isCenter ? 3 : 1.5,
      fillOpacity: 0.85
    })
    .bindTooltip(tip, { direction: 'top', offset: [0, -8] })
    .bindPopup(popup, { maxWidth: 400 });

    // Single click — if radius input has a value and not in draw mode, set this as center
    marker.on('click', function(e) {
      if (drawMode) return; // polygon drawing takes priority
      const radiusVal = parseFloat(document.getElementById('radius').value);
      if (radiusVal > 0) {
        radiusCenter = { lat: p.gps_lat, lng: p.gps_long, apn: p.apn };
        applyFilters();
      }
    });

    // Double-click → open property in Supabase
    marker.on('dblclick', function(e) {
      L.DomEvent.stopPropagation(e);
      window.open('https://supabase.com/dashboard/project/xanfkjitudcpduodadgu/editor?schema=public&table=properties&filter=id%3Aeq%3A' + p.id, '_blank');
    });

    markers.addLayer(marker);
  });
}

function filterPricingMatrix(minA, maxA, cid, radiusVal) {
  return (window.PRICING_MATRIX || []).filter(function(pm) {
    if (pm.acreage && pm.acreage < minA) return false;
    if (pm.acreage && maxA < Infinity && pm.acreage > maxA) return false;
    if (cid) {
      var matchCounty = (pm.county || '').toLowerCase().includes(cid);
      var matchCid = (pm.county_identifier || '').toLowerCase().includes(cid);
      if (!matchCounty && !matchCid) return false;
    }
    // Radius filter
    if (radiusCenter && radiusVal > 0) {
      var dist = haversine(radiusCenter.lat, radiusCenter.lng, pm.gps_lat, pm.gps_long);
      if (dist > radiusVal) return false;
    }
    // Polygon filter
    if (polyShape && polyPoints.length >= 3) {
      if (!pointInPolygon(pm.gps_lat, pm.gps_long, polyPoints)) return false;
    }
    return true;
  });
}

function applyFilters() {
  const minA = parseFloat(document.getElementById('minAcres').value) || 0;
  const maxA = parseFloat(document.getElementById('maxAcres').value) || Infinity;
  const cid = document.getElementById('countyId').value.trim().toLowerCase();
  const status = document.getElementById('status').value;
  const radiusVal = parseFloat(document.getElementById('radius').value) || 0;

  // Remove old radius circle
  if (radiusCircle) { map.removeLayer(radiusCircle); radiusCircle = null; }

  const filtered = window.PROPERTIES.filter(p => {
    if (p.acreage < minA) return false;
    if (p.acreage > maxA) return false;
    if (cid) {
      var mc = (p.county || '').toLowerCase().includes(cid);
      var mi = (p.county_identifier || '').toLowerCase().includes(cid);
      if (!mc && !mi) return false;
    }
    if (status && p.status !== status) return false;
    // Radius filter
    if (radiusCenter && radiusVal > 0) {
      const dist = haversine(radiusCenter.lat, radiusCenter.lng, p.gps_lat, p.gps_long);
      if (dist > radiusVal) return false;
    }
    // Polygon filter
    if (polyShape && polyPoints.length >= 3) {
      if (!pointInPolygon(p.gps_lat, p.gps_long, polyPoints)) return false;
    }
    return true;
  });

  const hasAreaFilter = (radiusCenter && radiusVal > 0) || (polyShape && polyPoints.length >= 3);

  // Draw radius circle if active
  if (radiusCenter && radiusVal > 0) {
    radiusCircle = L.circle([radiusCenter.lat, radiusCenter.lng], {
      radius: radiusVal * 1609.34, // miles to meters
      color: '#fbbf24', weight: 2, fillColor: '#fbbf24', fillOpacity: 0.08,
      dashArray: '6,4'
    }).addTo(map);
    document.getElementById('clearRadiusBtn').style.display = '';
    document.getElementById('radiusInfo').textContent = radiusVal + ' mi around ' + radiusCenter.apn + ' (' + filtered.length + ' found)';
    map.fitBounds(radiusCircle.getBounds(), { padding: [30, 30] });
  } else {
    document.getElementById('clearRadiusBtn').style.display = 'none';
    document.getElementById('radiusInfo').textContent = '';
  }

  // Polygon info
  if (polyShape && polyPoints.length >= 3) {
    document.getElementById('polyInfo').textContent = filtered.length + ' properties in area';
    if (!radiusCenter || radiusVal <= 0) {
      map.fitBounds(polyShape.getBounds(), { padding: [30, 30] });
    }
  }

  // Filter pricing matrix too
  const filteredPM = filterPricingMatrix(minA, maxA, cid, radiusVal);
  renderPricingMarkers(filteredPM);

  // Update pin count
  var countText = filtered.length + ' properties';
  if (filteredPM.length) countText += ' + ' + filteredPM.length + ' airtable';
  document.getElementById('pinCount').textContent = countText;

  // Show/hide stats for any area filter
  if (hasAreaFilter) {
    showRadiusStats(filtered, filteredPM);
  } else {
    hideRadiusStats();
  }

  renderMarkers(filtered);
}

function clearRadius() {
  radiusCenter = null;
  if (radiusCircle) { map.removeLayer(radiusCircle); radiusCircle = null; }
  document.getElementById('radius').value = '';
  document.getElementById('clearRadiusBtn').style.display = 'none';
  document.getElementById('radiusInfo').textContent = '';
  applyFilters();
}

function resetFilters() {
  document.getElementById('minAcres').value = '';
  document.getElementById('maxAcres').value = '';
  document.getElementById('countyId').value = '';
  document.getElementById('status').value = '';
  radiusCenter = null;
  if (radiusCircle) { map.removeLayer(radiusCircle); radiusCircle = null; }
  document.getElementById('radius').value = '';
  document.getElementById('clearRadiusBtn').style.display = 'none';
  document.getElementById('radiusInfo').textContent = '';
  // Clear polygon
  if (polyShape) { map.removeLayer(polyShape); polyShape = null; }
  clearPartialDraw();
  polyPoints = [];
  document.getElementById('clearPolyBtn').style.display = 'none';
  document.getElementById('closePolyBtn').style.display = 'none';
  document.getElementById('polyInfo').textContent = '';
  if (drawMode) {
    drawMode = false;
    document.getElementById('drawPolyBtn').textContent = 'Draw Area';
    document.getElementById('drawPolyBtn').style.background = '#8b5cf6';
    map.getContainer().style.cursor = '';
  }
  hideRadiusStats();
  renderMarkers(window.PROPERTIES);
  renderPricingMarkers(window.PRICING_MATRIX || []);
  var countText = window.PROPERTIES.length + ' properties';
  if (window.PRICING_MATRIX && window.PRICING_MATRIX.length) countText += ' + ' + window.PRICING_MATRIX.length + ' airtable';
  document.getElementById('pinCount').textContent = countText;
}

async function refreshData() {
  const btn = document.getElementById('refreshBtn');
  btn.textContent = 'Refreshing...';
  btn.disabled = true;
  try {
    const [propRes, pmRes] = await Promise.all([
      fetch('/api/properties'),
      fetch('/api/pricing-matrix')
    ]);
    if (!propRes.ok) throw new Error('Properties: ' + propRes.status);
    window.PROPERTIES = await propRes.json();
    if (pmRes.ok) window.PRICING_MATRIX = await pmRes.json();
    applyFilters();
    var cnt = window.PROPERTIES.length + ' + ' + (window.PRICING_MATRIX || []).length + ' loaded';
    btn.textContent = cnt;
    setTimeout(() => { btn.textContent = 'Refresh Data'; }, 3000);
  } catch(e) {
    btn.textContent = 'Refresh Failed';
    setTimeout(() => { btn.textContent = 'Refresh Data'; }, 3000);
  }
  btn.disabled = false;
}

// Initial render
renderMarkers(window.PROPERTIES);
renderPricingMarkers(window.PRICING_MATRIX || []);
var initCount = window.PROPERTIES.length + ' properties';
if (window.PRICING_MATRIX && window.PRICING_MATRIX.length) initCount += ' + ' + window.PRICING_MATRIX.length + ' airtable';
document.getElementById('pinCount').textContent = initCount;
<\/script>
</body>
</html>`;
}

// --- Server state ---
let cachedHTML = null;
let propertyCount = 0;
let pmCount = 0;
let generatedAt = null;

async function generateAndCache() {
  console.log('Fetching properties from Supabase...');
  const properties = await fetchProperties();
  propertyCount = properties.length;
  console.log(`Fetched ${propertyCount} properties (${properties.filter(p => p.airtable_prices && p.airtable_prices.length).length} enriched)`);

  console.log('Fetching pricing matrix...');
  const pricingMatrix = await fetchPricingMatrix();
  pmCount = pricingMatrix.length;
  console.log(`Fetched ${pmCount} unmatched pricing matrix entries`);

  generatedAt = new Date().toISOString();
  cachedHTML = buildHTML(properties, pricingMatrix);
  console.log(`Generated HTML at ${generatedAt}`);
}

const PORT = process.env.PORT || 3000;

async function startServer() {
  await generateAndCache();

  const server = http.createServer(async (req, res) => {
    if (req.url === '/' || req.url === '/index.html') {
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(cachedHTML);

    } else if (req.url === '/health') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: 'ok',
        properties: propertyCount,
        pricing_matrix: pmCount,
        generated: generatedAt
      }));

    } else if (req.url === '/api/properties') {
      try {
        const properties = await fetchProperties();
        res.writeHead(200, {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*'
        });
        res.end(JSON.stringify(properties));
      } catch (e) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: e.message }));
      }

    } else if (req.url === '/api/pricing-matrix') {
      try {
        const pm = await fetchPricingMatrix();
        res.writeHead(200, {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*'
        });
        res.end(JSON.stringify(pm));
      } catch (e) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: e.message }));
      }

    } else {
      res.writeHead(404);
      res.end('Not found');
    }
  });

  server.listen(PORT, () => {
    console.log(`Map server running on port ${PORT}`);
    console.log(`Properties: ${propertyCount} | Pricing Matrix: ${pmCount}`);
  });
}

startServer().catch(err => {
  console.error('Failed to start server:', err);
  process.exit(1);
});
