"""
N.E.D.S (Smart Lathi) - Raspberry Pi hub
  * Subscribes to ESP32 readings over MQTT (Mosquitto broker on this Pi)
  * Learns a clean-air baseline, scores each reading, raises alerts
  * Logs everything to SQLite
  * Serves a live dashboard at http://<pi-ip>:5000

Single-file version: the dashboard HTML is built in, no "static" folder needed.

Setup (once, in a terminal):
    sudo apt install -y python3-paho-mqtt python3-flask mosquitto mosquitto-clients
Run:
    python3 neds_pi.py      (or press Run in Thonny)
Then open http://localhost:5000 or http://<pi-ip>:5000
"""

import json
import os
import sqlite3
import threading
import time
from collections import deque

import paho.mqtt.client as mqtt
from flask import Flask, Response, jsonify

# ---------------- config ----------------
MQTT_HOST = "localhost"          # broker runs on the Pi itself
MQTT_PORT = 1883
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "neds_log.db")
MODEL_PATH = os.path.join(BASE_DIR, "model.pkl")   # optional trained model

BASELINE_SAMPLES = 30     # warm readings averaged to form clean-air baseline
CAUTION_RATIO = 1.3       # reading / baseline above this -> caution
ALERT_RATIO = 1.8         # above this -> alert
HISTORY_LEN = 300         # points kept for the live chart (~5 min at 1 Hz)
OFFLINE_AFTER_S = 5       # no data for this long -> device shown offline
# ----------------------------------------

app = Flask(__name__)
lock = threading.Lock()

state = {
    "device_id": None,
    "device_status": "waiting",   # waiting / online / offline
    "last_seen": 0.0,
    "latest": None,
    "baseline": None,             # {"mq2_mv": x, "mq5_mv": y}
    "baseline_progress": 0,
    "status": "calibrating",      # warming / calibrating / clear / caution / alert
    "score": 0.0,
    "ml_label": None,
    "events": deque(maxlen=20),
}
history = deque(maxlen=HISTORY_LEN)
_baseline_buf = []

# ---------- optional ML model ----------
model = None
if os.path.exists(MODEL_PATH):
    try:
        import joblib
        model = joblib.load(MODEL_PATH)
        print(f"[ml] loaded model from {MODEL_PATH}")
    except Exception as e:
        print(f"[ml] could not load model: {e}")


def ml_predict(mq2, mq5):
    """Hook for your trained classifier. Features must match what it was trained on."""
    if model is None:
        return None
    try:
        return str(model.predict([[mq2, mq5]])[0])
    except Exception as e:
        print(f"[ml] predict failed: {e}")
        return None


# ---------- database ----------
def db_init():
    with sqlite3.connect(DB_PATH) as db:
        db.execute("""CREATE TABLE IF NOT EXISTS readings (
            ts REAL, device TEXT, mq2_mv INTEGER, mq5_mv INTEGER,
            score REAL, status TEXT, ml_label TEXT)""")


def db_log(ts, device, mq2, mq5, score, status, ml_label):
    try:
        with sqlite3.connect(DB_PATH) as db:
            db.execute("INSERT INTO readings VALUES (?,?,?,?,?,?,?)",
                       (ts, device, mq2, mq5, score, status, ml_label))
    except sqlite3.Error as e:
        print(f"[db] {e}")


# ---------- detection ----------
def process_reading(data):
    global _baseline_buf
    now = time.time()
    mq2, mq5 = int(data["mq2_mv"]), int(data["mq5_mv"])
    warm = bool(data.get("warm", True))

    with lock:
        state["device_id"] = data.get("id", "unknown")
        state["device_status"] = "online"
        state["last_seen"] = now
        state["latest"] = {"mq2_mv": mq2, "mq5_mv": mq5,
                           "rssi": data.get("rssi"), "ts": now}

        if not warm:
            state["status"] = "warming"
            state["score"] = 0.0
        elif state["baseline"] is None:
            _baseline_buf.append((mq2, mq5))
            state["baseline_progress"] = len(_baseline_buf)
            state["status"] = "calibrating"
            if len(_baseline_buf) >= BASELINE_SAMPLES:
                n = len(_baseline_buf)
                state["baseline"] = {
                    "mq2_mv": max(1, sum(r[0] for r in _baseline_buf) / n),
                    "mq5_mv": max(1, sum(r[1] for r in _baseline_buf) / n),
                }
                _baseline_buf = []
                add_event(now, "info", "Clean-air baseline set")
        else:
            b = state["baseline"]
            score = max(mq2 / b["mq2_mv"], mq5 / b["mq5_mv"])
            prev = state["status"]
            if score >= ALERT_RATIO:
                status = "alert"
            elif score >= CAUTION_RATIO:
                status = "caution"
            else:
                status = "clear"
            state["score"] = round(score, 2)
            state["status"] = status
            if status != prev and status in ("caution", "alert"):
                add_event(now, status,
                          f"{status.capitalize()}: {score:.2f}x baseline "
                          f"(MQ-2 {mq2} mV, MQ-5 {mq5} mV)")
            elif status == "clear" and prev in ("caution", "alert"):
                add_event(now, "info", "Readings back to normal")

        state["ml_label"] = ml_predict(mq2, mq5)
        history.append({"ts": now, "mq2": mq2, "mq5": mq5})
        snapshot = (now, state["device_id"], mq2, mq5,
                    state["score"], state["status"], state["ml_label"])

    db_log(*snapshot)


def add_event(ts, level, text):
    state["events"].appendleft({"ts": ts, "level": level, "text": text})


# ---------- MQTT ----------
def on_connect(client, userdata, flags, rc, properties=None):
    print(f"[mqtt] connected (rc={rc})")
    client.subscribe("neds/+/sensors")
    client.subscribe("neds/+/status")


def on_message(client, userdata, msg):
    try:
        if msg.topic.endswith("/status"):
            with lock:
                state["device_status"] = msg.payload.decode()
            return
        process_reading(json.loads(msg.payload.decode()))
    except Exception as e:
        print(f"[mqtt] bad message on {msg.topic}: {e}")


def start_mqtt():
    try:  # paho-mqtt 2.x
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="neds-pi")
    except AttributeError:  # paho-mqtt 1.x
        client = mqtt.Client(client_id="neds-pi")
    client.on_connect = on_connect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=10)
    client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=30)
    client.loop_start()
    return client


# ---------- dashboard page ----------
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>N.E.D.S live monitor</title>
<style>
  :root {
    --rail-blue: #1F3A5F;
    --paper: #EEF1F4;
    --panel: #FFFFFF;
    --ink: #1B2430;
    --muted: #6B7785;
    --line: #D5DBE2;
    --green: #1E8C4E;
    --amber: #D99A00;
    --red: #C62F28;
    --off: #3A4452;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; background: var(--paper); color: var(--ink);
    font-family: "Segoe UI", "Noto Sans", system-ui, sans-serif;
    font-size: 16px; line-height: 1.45;
  }
  header {
    background: var(--rail-blue); color: #fff;
    padding: 14px 24px; display: flex; flex-wrap: wrap;
    justify-content: space-between; align-items: baseline; gap: 8px;
  }
  header h1 { margin: 0; font-size: 1.35rem; font-weight: 600; }
  header .device { font-size: 0.95rem; opacity: 0.9; }
  .dot { display: inline-block; width: 9px; height: 9px; border-radius: 50%;
         margin-right: 6px; background: var(--muted); vertical-align: 1px; }
  .dot.online { background: #6FE3A0; }
  .dot.offline { background: #FF8A80; }

  main {
    max-width: 1180px; margin: 0 auto; padding: 24px;
    display: grid; grid-template-columns: 260px 1fr; gap: 24px;
  }
  @media (max-width: 820px) { main { grid-template-columns: 1fr; } }

  /* the signal mast: the one bold element */
  .mast {
    background: var(--rail-blue); border-radius: 14px; padding: 22px 18px;
    display: flex; flex-direction: column; align-items: center; gap: 18px;
  }
  .lamps {
    background: #0F2238; border-radius: 60px; padding: 16px;
    display: flex; flex-direction: column; gap: 14px;
  }
  .lamp {
    width: 76px; height: 76px; border-radius: 50%; background: var(--off);
    transition: background 0.25s, box-shadow 0.25s;
  }
  .lamp.red.on   { background: var(--red);   box-shadow: 0 0 34px 6px rgba(198,47,40,0.7); }
  .lamp.amber.on { background: var(--amber); box-shadow: 0 0 34px 6px rgba(217,154,0,0.65); }
  .lamp.green.on { background: var(--green); box-shadow: 0 0 34px 6px rgba(30,140,78,0.65); }
  .lamp.red.on.pulse { animation: pulse 0.9s ease-in-out infinite; }
  @keyframes pulse { 50% { box-shadow: 0 0 10px 2px rgba(198,47,40,0.4); } }
  @media (prefers-reduced-motion: reduce) { .lamp.red.on.pulse { animation: none; } }

  .verdict { color: #fff; text-align: center; }
  .verdict strong { display: block; font-size: 1.6rem; font-weight: 700; }
  .verdict span { font-size: 0.95rem; opacity: 0.85; }

  button {
    font: inherit; font-size: 0.9rem; color: #fff; background: transparent;
    border: 1px solid rgba(255,255,255,0.5); border-radius: 8px;
    padding: 8px 14px; cursor: pointer;
  }
  button:hover { background: rgba(255,255,255,0.1); }
  button:focus-visible { outline: 3px solid #9CC3F0; outline-offset: 2px; }

  .right { display: flex; flex-direction: column; gap: 24px; min-width: 0; }
  .readings { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; }
  @media (max-width: 560px) { .readings { grid-template-columns: 1fr; } }
  .reading { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 16px 18px; }
  .reading h2 { margin: 0 0 4px; font-size: 0.95rem; font-weight: 600; color: var(--muted); }
  .reading .val { font-size: 2rem; font-weight: 700; font-variant-numeric: tabular-nums; }
  .reading .val small { font-size: 1rem; font-weight: 500; color: var(--muted); margin-left: 4px; }
  .reading .sub { font-size: 0.9rem; color: var(--muted); font-variant-numeric: tabular-nums; }

  section.box { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 16px 18px; }
  section.box h2 { margin: 0 0 10px; font-size: 1.05rem; font-weight: 600; }
  .legend { font-size: 0.9rem; color: var(--muted); margin-bottom: 6px; }
  .legend i { display: inline-block; width: 14px; height: 3px; vertical-align: middle; margin: 0 6px 0 12px; }
  canvas { width: 100%; height: 240px; display: block; }

  ul.events { list-style: none; margin: 0; padding: 0; }
  ul.events li { padding: 8px 0 8px 12px; border-left: 4px solid var(--line); margin-bottom: 6px; font-size: 0.95rem; }
  ul.events li.alert { border-color: var(--red); }
  ul.events li.caution { border-color: var(--amber); }
  ul.events li time { color: var(--muted); font-variant-numeric: tabular-nums; margin-right: 8px; }
  .empty { color: var(--muted); font-size: 0.95rem; }
</style>
</head>
<body>
<header>
  <h1>N.E.D.S live monitor</h1>
  <div class="device"><span class="dot" id="devDot"></span><span id="devText">Waiting for sensor node</span></div>
</header>

<main>
  <aside class="mast" aria-live="polite">
    <div class="lamps" aria-hidden="true">
      <div class="lamp red" id="lampRed"></div>
      <div class="lamp amber" id="lampAmber"></div>
      <div class="lamp green" id="lampGreen"></div>
    </div>
    <div class="verdict">
      <strong id="verdict">Starting up</strong>
      <span id="verdictSub">Connecting to sensors</span>
    </div>
    <button id="recal" type="button">Recalibrate in clean air</button>
  </aside>

  <div class="right">
    <div class="readings">
      <div class="reading">
        <h2>MQ-2 (smoke, LPG)</h2>
        <div class="val"><span id="mq2">&ndash;</span><small>mV</small></div>
        <div class="sub" id="mq2b">Baseline not set</div>
      </div>
      <div class="reading">
        <h2>MQ-5 (natural gas, LPG)</h2>
        <div class="val"><span id="mq5">&ndash;</span><small>mV</small></div>
        <div class="sub" id="mq5b">Baseline not set</div>
      </div>
      <div class="reading">
        <h2>Detection score</h2>
        <div class="val"><span id="score">&ndash;</span><small>&times; baseline</small></div>
        <div class="sub" id="ml">Model: not loaded</div>
      </div>
    </div>

    <section class="box">
      <h2>Last 5 minutes</h2>
      <div class="legend"><i style="background:#1F3A5F"></i>MQ-2<i style="background:#D99A00"></i>MQ-5</div>
      <canvas id="chart"></canvas>
    </section>

    <section class="box">
      <h2>Events</h2>
      <ul class="events" id="events"><li class="empty">No events yet.</li></ul>
    </section>
  </div>
</main>

<script>
const $ = id => document.getElementById(id);
const fmtTime = ts => new Date(ts * 1000).toLocaleTimeString();

const VERDICTS = {
  warming:     ["Warming up", "Sensor heaters stabilising, about 1 minute"],
  calibrating: ["Calibrating", "Learning clean-air baseline"],
  clear:       ["Clear", "No abnormal gas levels"],
  caution:     ["Caution", "Readings above normal, check the area"],
  alert:       ["Alert", "Strong response detected"],
};

function setLamps(status) {
  $("lampRed").className = "lamp red" + (status === "alert" ? " on pulse" : "");
  $("lampAmber").className = "lamp amber" + (["caution","warming","calibrating"].includes(status) ? " on" : "");
  $("lampGreen").className = "lamp green" + (status === "clear" ? " on" : "");
}

function drawChart(hist) {
  const c = $("chart"), dpr = window.devicePixelRatio || 1;
  const w = c.clientWidth, h = c.clientHeight;
  c.width = w * dpr; c.height = h * dpr;
  const g = c.getContext("2d"); g.scale(dpr, dpr);
  g.clearRect(0, 0, w, h);
  if (hist.length < 2) {
    g.fillStyle = "#6B7785"; g.font = "15px system-ui";
    g.fillText("Waiting for readings", 12, h / 2); return;
  }
  const pad = { l: 46, r: 8, t: 8, b: 20 };
  const vals = hist.flatMap(p => [p.mq2, p.mq5]);
  let lo = Math.min(...vals), hi = Math.max(...vals);
  const span = Math.max(hi - lo, 50); lo -= span * 0.1; hi += span * 0.1;
  const x = i => pad.l + (i / (hist.length - 1)) * (w - pad.l - pad.r);
  const y = v => pad.t + (1 - (v - lo) / (hi - lo)) * (h - pad.t - pad.b);

  g.strokeStyle = "#D5DBE2"; g.fillStyle = "#6B7785"; g.font = "12px system-ui"; g.lineWidth = 1;
  for (let k = 0; k <= 4; k++) {
    const v = lo + (hi - lo) * k / 4, yy = y(v);
    g.beginPath(); g.moveTo(pad.l, yy); g.lineTo(w - pad.r, yy); g.stroke();
    g.fillText(Math.round(v), 4, yy + 4);
  }
  [["mq2", "#1F3A5F"], ["mq5", "#D99A00"]].forEach(([key, col]) => {
    g.strokeStyle = col; g.lineWidth = 2; g.beginPath();
    hist.forEach((p, i) => i ? g.lineTo(x(i), y(p[key])) : g.moveTo(x(i), y(p[key])));
    g.stroke();
  });
}

async function refresh() {
  try {
    const s = await (await fetch("/api/state")).json();

    const online = s.device_status === "online";
    $("devDot").className = "dot " + (online ? "online" : s.device_status === "offline" ? "offline" : "");
    $("devText").textContent = s.device_id
      ? `${s.device_id}: ${online ? "online" : "offline"}` + (s.latest && s.latest.rssi != null ? `, WiFi ${s.latest.rssi} dBm` : "")
      : "Waiting for sensor node";

    const status = (!online && s.device_id) ? "offline" : s.status;
    if (status === "offline") {
      setLamps("none");
      $("verdict").textContent = "Sensor offline";
      $("verdictSub").textContent = "Check ESP32 power and WiFi";
    } else {
      setLamps(status);
      const [v, sub] = VERDICTS[status] || [status, ""];
      $("verdict").textContent = v;
      $("verdictSub").textContent = status === "calibrating"
        ? `${sub} (${s.baseline_progress}/${s.baseline_needed})` : sub;
    }

    if (s.latest) {
      $("mq2").textContent = s.latest.mq2_mv;
      $("mq5").textContent = s.latest.mq5_mv;
    }
    if (s.baseline) {
      $("mq2b").textContent = `Baseline ${Math.round(s.baseline.mq2_mv)} mV`;
      $("mq5b").textContent = `Baseline ${Math.round(s.baseline.mq5_mv)} mV`;
      $("score").textContent = s.score.toFixed(2);
    } else {
      $("mq2b").textContent = $("mq5b").textContent = "Baseline not set";
      $("score").textContent = "\u2013";
    }
    $("ml").textContent = s.ml_label ? `Model: ${s.ml_label}` : "Model: not loaded";

    const ev = $("events");
    ev.innerHTML = s.events.length ? "" : '<li class="empty">No events yet.</li>';
    s.events.forEach(e => {
      const li = document.createElement("li");
      li.className = e.level;
      const t = document.createElement("time"); t.textContent = fmtTime(e.ts);
      li.append(t, document.createTextNode(e.text));
      ev.append(li);
    });

    drawChart(s.history);
  } catch (err) {
    $("devDot").className = "dot offline";
    $("devText").textContent = "Dashboard can't reach the Pi server";
  }
}

$("recal").addEventListener("click", async () => {
  await fetch("/api/recalibrate", { method: "POST" });
  refresh();
});

refresh();
setInterval(refresh, 1000);
window.addEventListener("resize", refresh);
</script>
</body>
</html>
"""


# ---------- web ----------
@app.route("/")
def index():
    return Response(DASHBOARD_HTML, mimetype="text/html")


@app.route("/api/state")
def api_state():
    with lock:
        if (state["device_status"] == "online"
                and time.time() - state["last_seen"] > OFFLINE_AFTER_S):
            state["device_status"] = "offline"
        return jsonify({
            "device_id": state["device_id"],
            "device_status": state["device_status"],
            "last_seen": state["last_seen"],
            "latest": state["latest"],
            "baseline": state["baseline"],
            "baseline_progress": state["baseline_progress"],
            "baseline_needed": BASELINE_SAMPLES,
            "status": state["status"],
            "score": state["score"],
            "ml_label": state["ml_label"],
            "thresholds": {"caution": CAUTION_RATIO, "alert": ALERT_RATIO},
            "events": list(state["events"]),
            "history": list(history),
        })


@app.route("/api/recalibrate", methods=["POST"])
def api_recalibrate():
    global _baseline_buf
    with lock:
        state["baseline"] = None
        state["baseline_progress"] = 0
        state["status"] = "calibrating"
        _baseline_buf = []
        add_event(time.time(), "info", "Recalibration started, keep sensors in clean air")
    return jsonify({"ok": True})


if __name__ == "__main__":
    db_init()
    start_mqtt()
    print("[web] dashboard on http://0.0.0.0:5000")
    app.run(host="0.0.0.0", port=5000, threaded=True)
