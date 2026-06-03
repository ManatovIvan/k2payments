// Copyright (C) 2026 mx20022-runtime contributors
// SPDX-License-Identifier: AGPL-3.0-only

//! `mx20022-web` — a lightweight browser UI for MT ↔ MX message conversion.
//!
//! It serves a single self-contained HTML page and a small JSON endpoint that
//! wraps the `mx20022-translate` engine. No frontend build step is required.
//!
//! Run with `cargo run -p mx20022-web`; override the listen address with the
//! `MX20022_WEB_ADDR` environment variable (default `0.0.0.0:8080`).

use axum::{
    response::Html,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use mx20022_model::generated::{
    camt::camt_053_001_11 as camt053,
    pacs::{pacs_008_001_13 as pacs008, pacs_009_001_10 as pacs009},
};
use mx20022_translate::mappings::{
    camt053_to_mt940::camt053_to_mt940, mt103_to_pacs008::mt103_to_pacs008,
    mt202_to_pacs009::mt202_to_pacs009, mt940_to_camt053::mt940_to_camt053,
    pacs008_to_mt103::pacs008_to_mt103, pacs009_to_mt202::pacs009_to_mt202, TranslationWarnings,
};
use mx20022_translate::mt::{
    fields::mt103::parse_mt103, fields::mt202::parse_mt202, fields::mt940::parse_mt940,
    parser::parse,
};

/// Default creation timestamp used for MT → MX conversions when none is given.
const DEFAULT_CREATION_TIME: &str = "2000-01-01T00:00:00";
/// Default MX message identifier used when none is given.
const DEFAULT_MSG_ID: &str = "WEBMSG001";

/// Incoming translation request.
#[derive(Deserialize)]
struct TranslateRequest {
    /// Target format: `pacs008` | `mt103` | `pacs009` | `mt202` | `camt053` | `mt940`.
    to: String,
    /// Raw input message (MT text or MX XML).
    input: String,
    /// Optional MX message-ID override (MT → MX only).
    #[serde(default)]
    msg_id: Option<String>,
    /// Optional ISO-8601 creation timestamp override (MT → MX only).
    #[serde(default)]
    creation_time: Option<String>,
}

/// Translation response.
#[derive(Serialize)]
struct TranslateResponse {
    /// Whether the conversion succeeded.
    ok: bool,
    /// The converted output (MX XML or MT text) when `ok` is true.
    #[serde(skip_serializing_if = "String::is_empty")]
    output: String,
    /// Non-fatal mapping warnings (`[field] message`).
    warnings: Vec<String>,
    /// Error message when `ok` is false.
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Flatten translation warnings into displayable `[field] message` strings.
fn collect_warnings(w: &TranslationWarnings) -> Vec<String> {
    w.warnings
        .iter()
        .map(|x| format!("[{}] {}", x.field, x.message))
        .collect()
}

/// Perform a single conversion, returning the output text and any warnings.
fn translate(
    to: &str,
    input: &str,
    msg_id: &str,
    creation_time: &str,
) -> Result<(String, Vec<String>), String> {
    match to {
        // -------- MT → MX --------
        "pacs008" => {
            let msg = parse(input).map_err(|e| e.to_string())?;
            let mt = parse_mt103(&msg.block4).map_err(|e| e.to_string())?;
            let r = mt103_to_pacs008(&mt, msg_id, creation_time).map_err(|e| e.to_string())?;
            let xml = mx20022_parse::ser::to_string_with_declaration(&r.message)
                .map_err(|e| e.to_string())?;
            Ok((xml, collect_warnings(&r.warnings)))
        }
        "pacs009" => {
            let msg = parse(input).map_err(|e| e.to_string())?;
            let mt = parse_mt202(&msg.block4).map_err(|e| e.to_string())?;
            let r = mt202_to_pacs009(&mt, msg_id, creation_time).map_err(|e| e.to_string())?;
            let xml = mx20022_parse::ser::to_string_with_declaration(&r.message)
                .map_err(|e| e.to_string())?;
            Ok((xml, collect_warnings(&r.warnings)))
        }
        "camt053" => {
            let msg = parse(input).map_err(|e| e.to_string())?;
            let mt = parse_mt940(&msg.block4).map_err(|e| e.to_string())?;
            let r = mt940_to_camt053(&mt, msg_id, creation_time).map_err(|e| e.to_string())?;
            let xml = mx20022_parse::ser::to_string_with_declaration(&r.message)
                .map_err(|e| e.to_string())?;
            Ok((xml, collect_warnings(&r.warnings)))
        }
        // -------- MX → MT --------
        "mt103" => {
            let doc: pacs008::Document =
                mx20022_parse::de::from_str(input).map_err(|e| e.to_string())?;
            let r = pacs008_to_mt103(&doc).map_err(|e| e.to_string())?;
            Ok((r.message, collect_warnings(&r.warnings)))
        }
        "mt202" => {
            let doc: pacs009::Document =
                mx20022_parse::de::from_str(input).map_err(|e| e.to_string())?;
            let r = pacs009_to_mt202(&doc).map_err(|e| e.to_string())?;
            Ok((r.message, collect_warnings(&r.warnings)))
        }
        "mt940" => {
            let doc: camt053::Document =
                mx20022_parse::de::from_str(input).map_err(|e| e.to_string())?;
            let r = camt053_to_mt940(&doc).map_err(|e| e.to_string())?;
            Ok((r.message, collect_warnings(&r.warnings)))
        }
        other => Err(format!(
            "unknown target '{other}' — valid: pacs008, mt103, pacs009, mt202, camt053, mt940"
        )),
    }
}

/// Serve the single-page UI.
async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

/// JSON translation endpoint.
async fn api_translate(Json(req): Json<TranslateRequest>) -> Json<TranslateResponse> {
    if req.input.trim().is_empty() {
        return Json(TranslateResponse {
            ok: false,
            output: String::new(),
            warnings: Vec::new(),
            error: Some("input is empty".to_owned()),
        });
    }
    let msg_id = req
        .msg_id
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or(DEFAULT_MSG_ID);
    let ts = req
        .creation_time
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or(DEFAULT_CREATION_TIME);

    match translate(&req.to, &req.input, msg_id, ts) {
        Ok((output, warnings)) => Json(TranslateResponse {
            ok: true,
            output,
            warnings,
            error: None,
        }),
        Err(error) => Json(TranslateResponse {
            ok: false,
            output: String::new(),
            warnings: Vec::new(),
            error: Some(error),
        }),
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let addr = std::env::var("MX20022_WEB_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_owned());
    let app = Router::new()
        .route("/", get(index))
        .route("/api/translate", post(api_translate));

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind {addr}: {e}"));
    tracing::info!("mx20022-web listening on http://{addr}");
    axum::serve(listener, app).await.expect("server error");
}

/// The self-contained single-page UI (vanilla HTML/JS, no build step).
const INDEX_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>mx20022 — MT ↔ MX converter</title>
<style>
  :root { color-scheme: light dark; }
  body { font-family: system-ui, sans-serif; margin: 0; padding: 1.5rem; max-width: 1100px; margin-inline: auto; }
  h1 { font-size: 1.3rem; }
  .row { display: flex; gap: 1rem; flex-wrap: wrap; align-items: end; margin-bottom: .75rem; }
  label { display: block; font-size: .8rem; opacity: .8; margin-bottom: .25rem; }
  select, input { padding: .4rem; font: inherit; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
  textarea { width: 100%; height: 320px; box-sizing: border-box; font-family: ui-monospace, monospace; font-size: .8rem; padding: .5rem; }
  button { padding: .5rem 1rem; font: inherit; cursor: pointer; border-radius: 6px; border: 1px solid #8888; }
  button.primary { background: #2563eb; color: #fff; border-color: #2563eb; }
  .warn { color: #b45309; font-size: .8rem; white-space: pre-wrap; margin-top: .5rem; }
  .err { color: #dc2626; font-size: .85rem; white-space: pre-wrap; margin-top: .5rem; }
  .muted { opacity: .65; font-size: .8rem; }
  @media (max-width: 760px) { .grid { grid-template-columns: 1fr; } }
</style>
</head>
<body>
  <h1>mx20022 — конвертер SWIFT MT ↔ ISO 20022 MX</h1>
  <div class="row">
    <div>
      <label for="dir">Направление</label>
      <select id="dir">
        <optgroup label="MT → MX">
          <option value="pacs008">MT103 → pacs.008</option>
          <option value="pacs009">MT202 → pacs.009</option>
          <option value="camt053">MT940 → camt.053</option>
        </optgroup>
        <optgroup label="MX → MT">
          <option value="mt103">pacs.008 → MT103</option>
          <option value="mt202">pacs.009 → MT202</option>
          <option value="mt940">camt.053 → MT940</option>
        </optgroup>
      </select>
    </div>
    <div>
      <label for="file">Загрузить файл</label>
      <input id="file" type="file">
    </div>
    <div class="mtmx">
      <label for="msgid">MX msg-id (MT→MX)</label>
      <input id="msgid" placeholder="WEBMSG001">
    </div>
    <div class="mtmx">
      <label for="ts">Creation time (MT→MX)</label>
      <input id="ts" placeholder="2026-06-03T10:00:00">
    </div>
    <div>
      <button class="primary" id="go">Конвертировать</button>
    </div>
  </div>

  <div class="grid">
    <div>
      <label for="in">Вход</label>
      <textarea id="in" placeholder="Вставьте MT-текст или MX XML, либо загрузите файл выше…"></textarea>
    </div>
    <div>
      <label for="out">Результат
        <button id="dl" style="float:right;font-size:.75rem;padding:.15rem .5rem">Скачать</button>
      </label>
      <textarea id="out" readonly placeholder="Здесь появится результат…"></textarea>
      <div id="warn" class="warn"></div>
      <div id="err" class="err"></div>
    </div>
  </div>
  <p class="muted">Подсказка: msg-id и creation time используются только для направлений MT → MX.</p>

<script>
const $ = (id) => document.getElementById(id);
$("file").addEventListener("change", async (e) => {
  const f = e.target.files[0];
  if (f) $("in").value = await f.text();
});
$("dl").addEventListener("click", () => {
  const out = $("out").value;
  if (!out) return;
  const to = $("dir").value;
  const ext = (to === "mt103" || to === "mt202" || to === "mt940") ? "txt" : "xml";
  const blob = new Blob([out], { type: "text/plain" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = "result." + ext;
  a.click();
});
$("go").addEventListener("click", async () => {
  $("err").textContent = "";
  $("warn").textContent = "";
  $("out").value = "";
  const body = {
    to: $("dir").value,
    input: $("in").value,
    msg_id: $("msgid").value,
    creation_time: $("ts").value,
  };
  try {
    const res = await fetch("/api/translate", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    const data = await res.json();
    if (data.ok) {
      $("out").value = data.output;
      if (data.warnings && data.warnings.length)
        $("warn").textContent = "Предупреждения:\n" + data.warnings.join("\n");
    } else {
      $("err").textContent = "Ошибка: " + (data.error || "неизвестная");
    }
  } catch (e) {
    $("err").textContent = "Сетевая ошибка: " + e;
  }
});
</script>
</body>
</html>
"#;
