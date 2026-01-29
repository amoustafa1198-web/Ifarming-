package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;

import java.io.FileInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class Et0Job {

  // ---------- CONFIG (use ENV in production) ----------
  // Databoom
  private static final String DATABOOM_CHART_URL = "https://api.databoom.com/v1/chart";
  private static final String DATABOOM_PUSH_URL  = "https://api.databoom.com/v1/signals/push";

  // IDs of source signals in Databoom (read)
  private static final Map<String, String> IDS = Map.of(
      "temp", "67cae386932eacd225c7bb04",
      "hum",  "67cae386932eac1e85c7bb13",
      "wind", "67cae386932eac0ca3c7bb24",
      "rad",  "67cae386932eacc363c7bb17"
  );

  // Destination signal/device in Databoom (push)
  private static final String DEVICE_TOKEN = "IF2629-33E0715";
  private static final String ET0_SIGNAL_TOKEN = "ET0"; // ensure it's ET0 not ETO

  // Google Sheet
  // Put your full sheet ID (the long part in the URL)
  private static final String SHEET_ID = "1pAiT4tqKYrEv1tQ2YBR6xc_Yvkv_E0a6B-CO9bhllW0";
  private static final String SHEET_TAB_NAME = "ET0_PENMAN-MONTEITH"; // or "Sheet1"

  // ---------- ENV VARS ----------
  // DATABOOM_OAUTH_TOKEN : token for Bearer auth
  // GOOGLE_APPLICATION_CREDENTIALS : path to service_account.json (or you can hardcode a path)

  private static final ObjectMapper M = new ObjectMapper();
  private static final HttpClient HTTP = HttpClient.newHttpClient();

  public static void main(String[] args) throws Exception {
    String oauthToken = mustEnv("DATABOOM_OAUTH_TOKEN");

    // Yesterday in UTC
    LocalDate yesterday = LocalDate.now(ZoneOffset.UTC).minusDays(1);
    String startIso = yesterday + "T00:00:00Z";
    String endIso   = yesterday + "T23:59:59Z";

    // 1) Fetch data from Databoom
    JsonNode chartJson = fetchChart(oauthToken, startIso, endIso);

    List<Double> temp = extractValues(chartJson, IDS.get("temp"));
    List<Double> hum  = extractValues(chartJson, IDS.get("hum"));
    List<Double> wind = extractValues(chartJson, IDS.get("wind"));
    List<Double> rad  = extractValues(chartJson, IDS.get("rad"));

    if (temp.isEmpty() || hum.isEmpty() || wind.isEmpty() || rad.isEmpty()) {
      throw new IllegalStateException("Missing data: one or more signals returned empty series.");
    }

    // 2) Aggregate and compute ET0
    double tMax = temp.stream().mapToDouble(Double::doubleValue).max().orElseThrow();
    double tMin = temp.stream().mapToDouble(Double::doubleValue).min().orElseThrow();
    double hMean = mean(hum);
    double wMean = mean(wind);
    double rMean = mean(rad);

    double et0 = calcolaEt0Fao56(tMax, tMin, hMean, wMean, rMean, 100);
    System.out.println("Computed ET0 for " + yesterday + " = " + et0);

    // 3) Append to Google Sheet
    appendRowToSheet(
        yesterday.toString(),
        round2(tMax),
        round2(tMin),
        round2(hMean),
        round2(wMean),
        round2(rMean),
        round2(et0)
    );
    System.out.println("Appended row to Google Sheet.");

    // 4) Push ET0 back to Databoom
    // Use the same date stamp you want to appear in Databoom (e.g., 00:00 UTC of yesterday)
    String pushDate = yesterday + "T00:00:00Z";
    JsonNode pushResp = pushEt0(oauthToken, pushDate, et0);
    System.out.println("Databoom push response: " + pushResp.toString());
  }

  // ---------- Databoom: /v1/chart ----------
  private static JsonNode fetchChart(String oauthToken, String startIso, String endIso) throws Exception {
    ObjectNode payload = M.createObjectNode();
    payload.put("startDate", startIso);
    payload.put("endDate", endIso);
    payload.put("granularity", "h");

    payload.putArray("signals").addAll(
        IDS.values().stream().map(M::convertValue).collect(Collectors.toList())
    );

    HttpRequest req = HttpRequest.newBuilder()
        .uri(URI.create(DATABOOM_CHART_URL))
        .header("Authorization", "Bearer " + oauthToken)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .build();

    HttpResponse<String> res = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
    if (res.statusCode() < 200 || res.statusCode() >= 300) {
      throw new RuntimeException("Databoom chart failed: " + res.statusCode() + " " + res.body());
    }
    return M.readTree(res.body());
  }

  private static List<Double> extractValues(JsonNode chartJson, String signalId) {
    JsonNode series = chartJson.get(signalId);
    if (series == null || !series.isArray()) return List.of();

    List<Double> out = new ArrayList<>();
    for (JsonNode p : series) {
      JsonNode v = p.get("value");
      if (v != null && !v.isNull()) out.add(v.asDouble());
    }
    return out;
  }

  // ---------- ET0 FAO-56 ----------
  // Inputs:
  //  tMax/tMin: °C
  //  humMean: % (0-100)
  //  windKmhMean: km/h at 2 m (approx)
  //  radWattMean: W/m² mean
  private static double calcolaEt0Fao56(double tMax, double tMin, double humMean, double windKmhMean, double radWattMean, double altMeters) {
    double u2 = windKmhMean / 3.6;     // km/h -> m/s
    double rs = radWattMean * 0.0864;  // W/m² daily MJ/m²/day approximation

    double tMean = (tMax + tMin) / 2.0;

    double esTmax = 0.6108 * Math.exp((17.27 * tMax) / (tMax + 237.3));
    double esTmin = 0.6108 * Math.exp((17.27 * tMin) / (tMin + 237.3));
    double es = (esTmax + esTmin) / 2.0;

    double ea = es * (humMean / 100.0);

    double delta = (4098.0 * (0.6108 * Math.exp((17.27 * tMean) / (tMean + 237.3))))
        / Math.pow((tMean + 237.3), 2);

    double pAtm = 101.3 * Math.pow(((293.0 - 0.0065 * altMeters) / 293.0), 5.26);
    double gamma = 0.000665 * pAtm;

    double num = 0.408 * delta * rs + gamma * (900.0 / (tMean + 273.0)) * u2 * (es - ea);
    double den = delta + gamma * (1.0 + 0.34 * u2);

    return round2(num / den);
  }

  // ---------- Google Sheets append ----------
  private static void appendRowToSheet(String date, double tMax, double tMin, double humMean, double windMean, double radMean, double et0) throws Exception {
    String credsPath = mustEnv("GOOGLE_APPLICATION_CREDENTIALS");

    final NetHttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    GoogleCredentials creds = GoogleCredentials
        .fromStream(new FileInputStream(credsPath))
        .createScoped(List.of(SheetsScopes.SPREADSHEETS));

    Sheets sheets = new Sheets.Builder(httpTransport, GsonFactory.getDefaultInstance(), new HttpCredentialsAdapter(creds))
        .setApplicationName("ET0 Job")
        .build();

    // Append row to tab
    List<Object> row = List.of(date, tMax, tMin, humMean, windMean, radMean, et0);
    ValueRange body = new ValueRange().setValues(List.of(row));

    sheets.spreadsheets().values()
        .append(SHEET_ID, SHEET_TAB_NAME + "!A:G", body)
        .setValueInputOption("USER_ENTERED")
        .setInsertDataOption("INSERT_ROWS")
        .execute();
  }

  // ---------- Databoom push ET0 ----------
  private static JsonNode pushEt0(String oauthToken, String dateIso, double et0Value) throws Exception {
    ObjectNode payload = M.createObjectNode();
    payload.put("device", DEVICE_TOKEN);
    payload.put("date", dateIso);

    ObjectNode sig = M.createObjectNode();
    sig.put("name", ET0_SIGNAL_TOKEN);
    sig.put("value", et0Value);

    payload.putArray("signals").add(sig);

    HttpRequest req = HttpRequest.newBuilder()
        .uri(URI.create(DATABOOM_PUSH_URL))
        .header("Authorization", "Bearer " + oauthToken)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .build();

    HttpResponse<String> res = HTTP.send(req, HttpResponse.BodyHandlers.ofString());

    // Do not crash your whole job on transient Databoom errors:
    if (res.statusCode() < 200 || res.statusCode() >= 300) {
      ObjectNode err = M.createObjectNode();
      err.put("ok", false);
      err.put("status", res.statusCode());
      err.put("body", res.body());
      return err;
    }

    String body = res.body();
    if (body == null || body.isBlank()) {
      ObjectNode ok = M.createObjectNode();
      ok.put("ok", true);
      return ok;
    }
    return M.readTree(body);
  }

  // ---------- helpers ----------
  private static double mean(List<Double> xs) {
    return xs.stream().mapToDouble(Double::doubleValue).average().orElseThrow();
  }

  private static double round2(double x) {
    return Math.round(x * 100.0) / 100.0;
  }

  private static String mustEnv(String key) {
    String v = System.getenv(key);
    if (v == null || v.isBlank()) throw new IllegalStateException("Missing env var: " + key);
    return v;
  }
}
