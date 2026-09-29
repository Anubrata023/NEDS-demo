/*
 * ============================================================
 *  N.E.D.S (Smart Lathi) - ESP32 sensor node        FINAL
 * ============================================================
 *  Sends MQ-2 and MQ-5 readings to the Raspberry Pi over WiFi.
 *  Pairs with neds_pi.py running on the Pi.
 *
 *  BEFORE UPLOADING, EDIT ONLY THE 3 LINES MARKED "EDIT THIS"
 *  BELOW. Everything else is already set for your setup.
 *
 *  Arduino IDE settings:
 *    Tools > Board  : ESP32 Dev Module   (esp32 by Espressif)
 *    Tools > Port   : COM3               (your port)
 *    Serial Monitor : 115200 baud
 *
 *  Library needed: PubSubClient by Nick O'Leary
 *    (Sketch > Include Library > Manage Libraries > search
 *     "PubSubClient")
 *
 * ------------------------------------------------------------
 *  WIRING
 * ------------------------------------------------------------
 *  MQ modules output up to 5V. The ESP32 ADC only takes 3.3V,
 *  so each AOUT needs a 10k/10k voltage divider (halves it):
 *
 *    MQ-2 AOUT --[10k]--+--> GPIO34
 *                       |
 *                     [10k]
 *                       |
 *                      GND
 *
 *    MQ-5 AOUT --[10k]--+--> GPIO35
 *                       |
 *                     [10k]
 *                       |
 *                      GND
 *
 *  POWER (matches the final circuit diagram):
 *    MQ VCC -> Raspberry Pi 5V header pin 2  (NOT a 3.3V pin)
 *    MQ GND -> Raspberry Pi GND pin 6        (common ground)
 *    ESP32  -> plugged into a Pi USB-A port
 *    MQ D0  -> leave unconnected
 *
 *  Never join laptop USB 5V to the Pi 5V. Grounds yes, 5V rails no.
 *
 *  Use only GPIO 32-39 for sensors. The other ADC pins stop
 *  working once WiFi is switched on.
 * ============================================================
 */
#include <WiFi.h>
#include <PubSubClient.h>

// ============================================================
//  EDIT THIS SECTION
// ============================================================
// 1. EDIT THIS - your WiFi network name (must be 2.4 GHz)
const char* WIFI_SSID = "Priyu";
const char* WIFI_PASS = "12345678";
const char* MQTT_HOST = "10.195.40.139";
// ============================================================
//  No changes needed below this line
// ============================================================

const uint16_t MQTT_PORT = 1883;      // Mosquitto port on the Pi
const char* DEVICE_ID = "neds-01";    // shown on the dashboard
const int MQ2_PIN = 34;               // MQ-2 through its divider
const int MQ5_PIN = 35;               // MQ-5 through its divider
const int LED_PIN = 2;                // on-board blue LED

const unsigned long PUBLISH_MS = 1000;   // send one reading per second
const unsigned long WARMUP_MS  = 60000;  // MQ heaters settle for 60 s
const int SAMPLES = 32;                  // readings averaged per sample

// 10k on top, 10k to ground -> the pin sees half the real
// voltage, so multiply back by 2 to report the true value.
// Change this ONLY if you use different resistors:
//   10k / 20k -> 30.0 / 20.0        10k / 22k -> 32.0 / 22.0
const float DIVIDER_RATIO = 2.0;

WiFiClient net;
PubSubClient mqtt(net);

char topicData[64];
char topicStatus[64];
unsigned long lastPublish = 0;
unsigned long lastMqttTry = 0;
bool wasConnected = false;

// Averages several ADC samples and undoes the divider,
// so the value returned is the sensor's real output in mV.
int readMilliVolts(int pin) {
  long sum = 0;
  for (int i = 0; i < SAMPLES; i++) {
    sum += analogReadMilliVolts(pin);
    delayMicroseconds(500);
  }
  return (int)((sum / SAMPLES) * DIVIDER_RATIO);
}

// Turns an MQTT error number into something readable,
// so the Serial Monitor tells you what actually went wrong.
const char* mqttError(int state) {
  switch (state) {
    case -4: return "server did not respond in time";
    case -3: return "network connection was lost";
    case -2: return "cannot reach the Pi - check IP, WiFi, and Mosquitto";
    case -1: return "disconnected cleanly";
    case  1: return "wrong MQTT protocol version";
    case  2: return "device ID rejected";
    case  3: return "broker unavailable";
    case  4: return "bad username or password";
    case  5: return "not authorised by the broker";
    default: return "unknown";
  }
}

// Lists every network in range. Runs automatically when a
// connection attempt fails, so you can see the exact spelling
// of the networks the ESP32 can actually reach.
void scanNetworks() {
  Serial.println("Scanning for networks the ESP32 can see...");
  int n = WiFi.scanNetworks();
  if (n <= 0) {
    Serial.println("  none found - move closer to the router/hotspot");
  } else {
    for (int i = 0; i < n; i++) {
      Serial.printf("  \"%s\"  %d dBm  ch%d  %s\n",
                    WiFi.SSID(i).c_str(), WiFi.RSSI(i), WiFi.channel(i),
                    WiFi.encryptionType(i) == WIFI_AUTH_OPEN
                      ? "OPEN" : "needs password");
    }
    Serial.printf("Looking for \"%s\" in that list. If it is missing,\n", WIFI_SSID);
    Serial.println("the name is wrong or the network is 5 GHz only.");
  }
  WiFi.scanDelete();
}

void connectWiFi() {
  if (WiFi.status() == WL_CONNECTED) return;

  // Clear any half-open attempt first. Without this the ESP32
  // prints "cannot set config" and never finishes connecting.
  WiFi.persistent(false);
  WiFi.disconnect(true, true);
  WiFi.mode(WIFI_OFF);
  delay(300);
  WiFi.mode(WIFI_STA);
  delay(200);

  Serial.println();
  Serial.printf("Connecting to WiFi \"%s\"", WIFI_SSID);
  WiFi.begin(WIFI_SSID, WIFI_PASS);

  unsigned long start = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - start < 15000) {
    delay(500);
    Serial.print(".");
    digitalWrite(LED_PIN, !digitalRead(LED_PIN));
  }
  digitalWrite(LED_PIN, LOW);
  Serial.println();

  if (WiFi.status() == WL_CONNECTED) {
    Serial.printf("WiFi OK. ESP32 IP: %s  (signal %d dBm)\n",
                  WiFi.localIP().toString().c_str(), WiFi.RSSI());
    return;
  }

  // Explain exactly why it failed, then show what IS in range.
  switch (WiFi.status()) {
    case WL_NO_SSID_AVAIL:
      Serial.println("FAILED: that network name was not found."); break;
    case WL_CONNECT_FAILED:
      Serial.println("FAILED: wrong password."); break;
    default:
      Serial.printf("FAILED: status %d\n", WiFi.status()); break;
  }
  scanNetworks();
}

void connectMqtt() {
  if (mqtt.connected()) return;
  if (millis() - lastMqttTry < 3000) return;   // retry every 3 s
  lastMqttTry = millis();

  Serial.printf("Connecting to Pi at %s:%u ... ", MQTT_HOST, MQTT_PORT);

  // The "last will" tells the broker to mark this device offline
  // automatically if it loses power or drops off the network.
  if (mqtt.connect(DEVICE_ID, topicStatus, 0, true, "offline")) {
    Serial.println("CONNECTED");
    mqtt.publish(topicStatus, "online", true);
    wasConnected = true;
  } else {
    int s = mqtt.state();
    Serial.printf("FAILED (state %d: %s)\n", s, mqttError(s));
  }
}

void setup() {
  Serial.begin(115200);
  delay(500);
  pinMode(LED_PIN, OUTPUT);
  digitalWrite(LED_PIN, LOW);

  Serial.println();
  Serial.println("=====================================");
  Serial.println(" N.E.D.S sensor node starting");
  Serial.printf(" Device ID: %s\n", DEVICE_ID);
  Serial.println("=====================================");

  analogReadResolution(12);
  analogSetPinAttenuation(MQ2_PIN, ADC_11db);   // full input range
  analogSetPinAttenuation(MQ5_PIN, ADC_11db);

  snprintf(topicData, sizeof(topicData), "neds/%s/sensors", DEVICE_ID);
  snprintf(topicStatus, sizeof(topicStatus), "neds/%s/status", DEVICE_ID);

  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setBufferSize(512);
  mqtt.setKeepAlive(30);

  connectWiFi();
  Serial.println("Sensors warming up for 60 seconds...");
}

void loop() {
  // Reconnect automatically if WiFi drops
  if (WiFi.status() != WL_CONNECTED) {
    if (wasConnected) {
      Serial.println("WiFi lost. Reconnecting...");
      wasConnected = false;
    }
    connectWiFi();
    return;
  }

  connectMqtt();
  mqtt.loop();

  if (millis() - lastPublish < PUBLISH_MS) return;
  lastPublish = millis();

  int mq2 = readMilliVolts(MQ2_PIN);
  int mq5 = readMilliVolts(MQ5_PIN);
  bool warm = millis() > WARMUP_MS;

  char payload[200];
  snprintf(payload, sizeof(payload),
           "{\"id\":\"%s\",\"uptime_ms\":%lu,\"mq2_mv\":%d,\"mq5_mv\":%d,"
           "\"warm\":%s,\"rssi\":%d}",
           DEVICE_ID, millis(), mq2, mq5,
           warm ? "true" : "false", WiFi.RSSI());

  Serial.println(payload);

  if (mqtt.connected()) {
    mqtt.publish(topicData, payload);
    digitalWrite(LED_PIN, HIGH);    // blink once per successful send
    delay(20);
    digitalWrite(LED_PIN, LOW);
  }
}
