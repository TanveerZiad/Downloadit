use js_sys::{Date, Function, Object, Promise, Reflect};
use serde::Deserialize;
use std::cell::RefCell;
use wasm_bindgen::{closure::Closure, prelude::wasm_bindgen, JsCast, JsValue};
use wasm_bindgen_futures::{JsFuture, spawn_local};
use web_sys::{
    Document, Element, Event, EventSource, HtmlAnchorElement, HtmlDialogElement, HtmlInputElement,
    HtmlOptionElement, HtmlSelectElement, MessageEvent, Request, RequestInit, Response, Window,
};

thread_local! {
    static ANALYSIS: RefCell<Option<JsValue>> = RefCell::new(None);
    static JOBS: RefCell<Vec<Job>> = RefCell::new(Vec::new());
    static FOLDER: RefCell<Option<JsValue>> = RefCell::new(None);
    static FILTER: RefCell<String> = RefCell::new(String::new());
}

#[derive(Clone, Deserialize)]
struct VideoQuality {
    label: String,
    selector: String,
    size: Option<u64>,
    #[serde(default)]
    needs_ffmpeg: bool,
}

#[derive(Clone, Deserialize)]
struct Segment {
    start: u64,
    end: u64,
    pos: u64,
}

#[derive(Clone, Deserialize)]
struct Job {
    id: String,
    name: String,
    mime: String,
    size: i64,
    downloaded: u64,
    status: String,
    scan: String,
    error: String,
    updated: u64,
    #[serde(default)]
    speed: u64,
    #[serde(default)]
    eta: i64,
    #[serde(default)]
    segments: Vec<Segment>,
    #[serde(default)]
    url: String,
}

#[wasm_bindgen(start)]
pub fn start() {
    FILTER.with(|f| *f.borrow_mut() = "all".into());
    if let Err(e) = wire() {
        web_sys::console::error_1(&e);
    }
}

fn window() -> Window {
    web_sys::window().expect("window")
}
fn document() -> Document {
    window().document().expect("document")
}
fn by_id(id: &str) -> Element {
    document()
        .get_element_by_id(id)
        .expect("required UI element")
}
fn set_text(id: &str, text: &str) {
    if let Some(el) = document().get_element_by_id(id) {
        el.set_text_content(Some(text));
    }
}
fn set_html(id: &str, html: &str) {
    if let Some(el) = document().get_element_by_id(id) {
        el.set_inner_html(html);
    }
}
fn on(target: &Element, name: &str, callback: impl FnMut(Event) + 'static) {
    let cb = Closure::<dyn FnMut(Event)>::new(callback);
    let _ = target.add_event_listener_with_callback(name, cb.as_ref().unchecked_ref());
    cb.forget();
}

fn wire() -> Result<(), JsValue> {
    let form = by_id("analyze-form");
    on(&form, "submit", |e| {
        e.prevent_default();
        spawn_local(async { analyze().await });
    });
    on(&by_id("download-button"), "click", |_| {
        spawn_local(async { start_download().await })
    });
    on(&by_id("cancel-analysis"), "click", |_| {
        ANALYSIS.with(|a| *a.borrow_mut() = None);
        let _ = by_id("analysis-card").class_list().add_1("hidden");
        if let Some(opts) = document().get_element_by_id("video-options") {
            let _ = opts.class_list().add_1("hidden");
        }
    });
    on(&by_id("paste-button"), "click", |_| {
        spawn_local(async {
            let nav = window().navigator();
            let promise = nav.clipboard().read_text();
            match JsFuture::from(promise).await {
                Ok(v) => {
                    if let Some(input) = document()
                        .get_element_by_id("url-input")
                        .and_then(|e| e.dyn_into::<HtmlInputElement>().ok())
                    {
                        input.set_value(&v.as_string().unwrap_or_default());
                    }
                }
                Err(_) => {
                    toast("Paste your URL into the field.");
                }
            }
        })
    });
    on(&by_id("job-list"), "click", |e| {
        let Some(target) = e.target().and_then(|x| x.dyn_into::<Element>().ok()) else {
            return;
        };
        if let Some(id) = target.get_attribute("data-cancel") {
            spawn_local(async move { cancel_job(&id).await });
        } else if let Some(id) = target.get_attribute("data-pause") {
            spawn_local(async move { pause_job(&id).await });
        } else if let Some(id) = target.get_attribute("data-resume") {
            spawn_local(async move { resume_job(&id).await });
        } else if let Some(id) = target.get_attribute("data-save") {
            save_job(&id);
        } else if let Some(url) = target.get_attribute("data-copy") {
            let nav = window().navigator();
            let _ = nav.clipboard().write_text(&url);
            toast("Link copied to clipboard.");
        }
    });
    if let Some(tabs) = document().get_element_by_id("filter-tabs") {
        on(&tabs, "click", |e| {
            let Some(target) = e.target().and_then(|x| x.dyn_into::<Element>().ok()) else {
                return;
            };
            let tab_el = if target.has_attribute("data-filter") {
                Some(target)
            } else {
                target.closest("[data-filter]").ok().flatten()
            };
            if let Some(tab) = tab_el {
                if let Some(filter) = tab.get_attribute("data-filter") {
                    FILTER.with(|f| *f.borrow_mut() = filter.clone());
                    if let Ok(tabs_list) = document().query_selector_all(".filter-tab") {
                        for i in 0..tabs_list.length() {
                            if let Some(el) = tabs_list.item(i).and_then(|n| n.dyn_into::<Element>().ok()) {
                                let active = el
                                    .get_attribute("data-filter")
                                    .as_deref() == Some(filter.as_str());
                                let _ = el.class_list().toggle_with_force("active", active);
                            }
                        }
                    }
                    render();
                }
            }
        });
    }
    on(&by_id("clear-completed"), "click", |_| {
        JOBS.with(|jobs| {
            jobs.borrow_mut()
                .retain(|j| !["completed", "failed", "cancelled"].contains(&j.status.as_str()))
        });
        render();
    });
    if let Some(dialog) = document()
        .get_element_by_id("settings-dialog")
        .and_then(|e| e.dyn_into::<HtmlDialogElement>().ok())
    {
        let d = dialog.clone();
        on(&by_id("settings-open"), "click", move |_| {
            let _ = d.show_modal();
        });
        let d_close = dialog.clone();
        on(&dialog, "click", move |e| {
            if let Some(target) = e.target().and_then(|t| t.dyn_into::<Element>().ok()) {
                if target.class_list().contains("close-dialog") {
                    e.prevent_default();
                    d_close.close();
                }
            }
        });
    }
    let history = window()
        .local_storage()
        .ok()
        .flatten()
        .and_then(|s| s.get_item("downloadit-history").ok().flatten())
        .as_deref()
        != Some("false");
    if let Some(input) = document()
        .get_element_by_id("history-setting")
        .and_then(|e| e.dyn_into::<HtmlInputElement>().ok())
    {
        input.set_checked(history);
        on(
            &input,
            "change",
            move |_| {
                if let Some(i) = document()
                    .get_element_by_id("history-setting")
                    .and_then(|e| e.dyn_into::<HtmlInputElement>().ok())
                {
                    if let Ok(Some(s)) = window().local_storage() {
                        let _ = s.set_item(
                            "downloadit-history",
                            if i.checked() { "true" } else { "false" },
                        );
                    }
                }
            },
        );
    }
    if let Some(folder_btn) = document().get_element_by_id("hero-folder-button") {
        on(&folder_btn, "click", |_| {
            match pick_folder_promise() {
                Ok(promise) => {
                    spawn_local(async move {
                        match JsFuture::from(promise).await {
                            Ok(res) => apply_picked_folder(res),
                            Err(err) => {
                                let err_str = js_error(err);
                                if !err_str.to_lowercase().contains("abort") {
                                    toast("Folder access was not granted.");
                                }
                            }
                        }
                    });
                }
                Err(_) => {
                    if let Some(dialog) = document()
                        .get_element_by_id("settings-dialog")
                        .and_then(|e| e.dyn_into::<HtmlDialogElement>().ok())
                    {
                        let _ = dialog.show_modal();
                    }
                    toast("Folder picker requires Chrome, Edge, or Brave.");
                }
            }
        });
    }
    if let Some(input) = document()
        .get_element_by_id("folder-setting")
        .and_then(|e| e.dyn_into::<HtmlInputElement>().ok())
    {
        on(&input, "change", |e| {
            let Some(target) = e
                .target()
                .and_then(|t| t.dyn_into::<HtmlInputElement>().ok())
            else {
                return;
            };
            if !target.checked() {
                FOLDER.with(|f| *f.borrow_mut() = None);
                update_folder_ui(None);
                toast("Save location cleared (using browser default).");
                return;
            }
            match pick_folder_promise() {
                Ok(promise) => {
                    spawn_local(async move {
                        match JsFuture::from(promise).await {
                            Ok(res) => apply_picked_folder(res),
                            Err(_) => {
                                target.set_checked(false);
                                update_folder_ui(None);
                                toast("Folder access was not granted.");
                            }
                        }
                    });
                }
                Err(_) => {
                    target.set_checked(false);
                    update_folder_ui(None);
                    toast("Folder picker requires Chrome, Edge, or Brave.");
                }
            }
        });
    }
    spawn_local(refresh());
    Ok(())
}

async fn api(path: &str, method: &str, body: Option<String>) -> Result<JsValue, String> {
    let init = RequestInit::new();
    init.set_method(method);
    if let Some(b) = body {
        init.set_body(&JsValue::from_str(&b));
    }
    let req = Request::new_with_str_and_init(&format!("/api/v1{path}"), &init).map_err(js_error)?;
    if method != "GET" {
        req.headers()
            .set("Content-Type", "application/json")
            .map_err(js_error)?;
    }
    let value = JsFuture::from(window().fetch_with_request(&req))
        .await
        .map_err(js_error)?;
    let response: Response = value.dyn_into().map_err(js_error)?;
    let data = JsFuture::from(response.json().map_err(js_error)?)
        .await
        .map_err(js_error)?;
    if !response.ok() {
        let message = reflect_get(&data, "error")
            .ok()
            .and_then(|v| v.as_string())
            .unwrap_or_else(|| format!("Request failed ({})", response.status()));
        return Err(message);
    }
    Ok(data)
}

fn js_error(e: JsValue) -> String {
    e.as_string().unwrap_or_else(|| "Request failed".into())
}

async fn analyze() {
    let Some(input) = document()
        .get_element_by_id("url-input")
        .and_then(|e| e.dyn_into::<HtmlInputElement>().ok())
    else {
        return;
    };
    let url = input.value();
    if url.trim().is_empty() {
        return;
    }
    set_html_button("analyze-form", "Checking link…");
    match api(
        "/analyze",
        "POST",
        Some(serde_json::json!({ "url": url }).to_string()),
    )
    .await
    {
        Ok(data) => {
            let name = prop(&data, "name");
            let mime = prop(&data, "mime");
            let size_lbl = prop(&data, "size_label");
            let kind = prop(&data, "kind");
            let is_video = kind == "video";

            set_text("analysis-name", &name);

            let type_el = by_id("analysis-type");
            if is_video {
                set_text("analysis-type", "VIDEO");
                let _ = type_el.class_list().add_1("video");
            } else {
                let _ = type_el.class_list().remove_1("video");
                set_text(
                    "analysis-type",
                    name.rsplit('.')
                        .next()
                        .unwrap_or("FILE")
                        .to_uppercase()
                        .get(..6)
                        .unwrap_or("FILE"),
                );
            }

            if let Some(sel) = document()
                .get_element_by_id("quality-select")
                .and_then(|e| e.dyn_into::<HtmlSelectElement>().ok())
            {
                sel.set_inner_html("");
                let qualities_val = reflect_get(&data, "qualities").unwrap_or(JsValue::UNDEFINED);
                let qualities: Vec<VideoQuality> =
                    serde_wasm_bindgen::from_value(qualities_val).unwrap_or_default();

                if is_video && !qualities.is_empty() {
                    for q in &qualities {
                        if let Ok(opt) = document().create_element("option") {
                            if let Ok(opt_el) = opt.dyn_into::<HtmlOptionElement>() {
                                opt_el.set_value(&q.selector);
                                let size_part = q
                                    .size
                                    .map(|s| format!(" (~{})", size(s as i64)))
                                    .unwrap_or_default();
                                opt_el.set_text_content(Some(&format!("{}{}", q.label, size_part)));
                                let _ = sel.append_child(&opt_el);
                            }
                        }
                    }
                    if let Some(video_box) = document().get_element_by_id("video-options") {
                        let _ = video_box.class_list().remove_1("hidden");
                    }
                } else if let Some(video_box) = document().get_element_by_id("video-options") {
                    let _ = video_box.class_list().add_1("hidden");
                }
            }

            let conns = reflect_get(&data, "connections")
                .ok()
                .and_then(|v| v.as_f64())
                .unwrap_or(1.0) as u32;

            let conn_label = if conns > 1 {
                format!(" · {conns} connections")
            } else {
                String::new()
            };
            set_text("analysis-meta", &format!("{mime} · {size_lbl}{conn_label}"));

            ANALYSIS.with(|a| *a.borrow_mut() = Some(data));
            let _ = by_id("analysis-card").class_list().remove_1("hidden");
        }
        Err(e) => toast(&e),
    }
    set_html_button("analyze-form", "Check link");
}

fn set_html_button(_form: &str, label: &str) {
    if let Ok(Some(button)) = document().query_selector(".analyze-button") {
        button.set_inner_html(&format!("<span>{}</span><span>→</span>", escape(label)));
    }
}

fn update_folder_ui(folder_name: Option<&str>) {
    if let Some(btn) = document().get_element_by_id("hero-folder-button") {
        if let Some(name) = folder_name {
            let _ = btn.class_list().add_1("has-folder");
            let _ = btn.set_attribute("title", &format!("Save location: {name} (click to change)"));
            if let Some(dot) = document().get_element_by_id("folder-dot") {
                let _ = dot.class_list().remove_1("hidden");
            }
        } else {
            let _ = btn.class_list().remove_1("has-folder");
            let _ = btn.set_attribute("title", "Choose save location");
            if let Some(dot) = document().get_element_by_id("folder-dot") {
                let _ = dot.class_list().add_1("hidden");
            }
        }
    }
}

fn apply_picked_folder(res: JsValue) {
    let is_unsupported = reflect_get(&res, "unsupported")
        .ok()
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if is_unsupported {
        toast("Folder access requires Chrome, Edge, or Brave.");
        return;
    }

    let is_cancelled = reflect_get(&res, "cancelled")
        .ok()
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if is_cancelled {
        return;
    }

    if let Ok(err_val) = reflect_get(&res, "error") {
        if let Some(err_msg) = err_val.as_string() {
            toast(&format!("Folder access: {err_msg}"));
            return;
        }
    }

    let (handle, name) = if let Ok(h) = reflect_get(&res, "handle") {
        if !h.is_undefined() && !h.is_null() {
            let n = reflect_get(&res, "name")
                .ok()
                .and_then(|v| v.as_string())
                .unwrap_or_else(|| "Selected folder".to_string());
            (h, n)
        } else {
            let n = reflect_get(&res, "name")
                .ok()
                .and_then(|v| v.as_string())
                .unwrap_or_else(|| "Selected folder".to_string());
            (res, n)
        }
    } else {
        let n = reflect_get(&res, "name")
            .ok()
            .and_then(|v| v.as_string())
            .unwrap_or_else(|| "Selected folder".to_string());
        (res, n)
    };

    FOLDER.with(|f| *f.borrow_mut() = Some(handle));
    update_folder_ui(Some(&name));
    if let Some(chk) = document()
        .get_element_by_id("folder-setting")
        .and_then(|el| el.dyn_into::<HtmlInputElement>().ok())
    {
        chk.set_checked(true);
    }
    toast(&format!("Save location set to: {name}"));
}

async fn start_download() {
    let analysis = ANALYSIS.with(|a| a.borrow().clone());
    let Some(a) = analysis else { return };
    let url = prop(&a, "url");
    let name = prop(&a, "name");

    let is_video_visible = document()
        .get_element_by_id("video-options")
        .map(|el| !el.class_list().contains("hidden"))
        .unwrap_or(false);

    let selector = if is_video_visible {
        document()
            .get_element_by_id("quality-select")
            .and_then(|e| e.dyn_into::<HtmlSelectElement>().ok())
            .map(|s| s.value())
            .filter(|v| !v.is_empty())
    } else {
        None
    };

    let payload = if let Some(sel) = selector {
        serde_json::json!({ "url": url, "name": name, "selector": sel })
    } else {
        serde_json::json!({ "url": url, "name": name })
    };

    match api(
        "/downloads",
        "POST",
        Some(payload.to_string()),
    )
    .await
    {
        Ok(job) => {
            let id = prop(&job, "id");
            let _ = by_id("analysis-card").class_list().add_1("hidden");
            if let Some(opts) = document().get_element_by_id("video-options") {
                let _ = opts.class_list().add_1("hidden");
            }
            ANALYSIS.with(|a| *a.borrow_mut() = None);
            if let Some(i) = document()
                .get_element_by_id("url-input")
                .and_then(|e| e.dyn_into::<HtmlInputElement>().ok())
            {
                i.set_value("");
            }
            spawn_local(refresh());
            watch(&id);
            toast("Download added to your queue.");
        }
        Err(e) => toast(&e),
    }
}

async fn refresh() {
    match api("/downloads", "GET", None).await {
        Ok(v) => {
            let values = reflect_get(&v, "downloads").unwrap_or(JsValue::UNDEFINED);
            let parsed: Vec<Job> = serde_wasm_bindgen::from_value(values).unwrap_or_default();
            JOBS.with(|j| *j.borrow_mut() = parsed.clone());
            render();
            for j in parsed {
                if ["queued", "downloading", "scanning"].contains(&j.status.as_str()) {
                    watch(&j.id);
                }
            }
        }
        Err(e) => toast(&e),
    }
}

fn watch(id: &str) {
    let Ok(es) = EventSource::new(&format!("/api/v1/downloads/{id}/events")) else {
        return;
    };
    let event_source = es.clone();
    let closure = Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
        if let Some(data) = event.data().as_string() {
            if let Ok(v) = serde_json::from_str::<Job>(&data) {
                let done = ["completed", "failed", "cancelled"].contains(&v.status.as_str());
                JOBS.with(|jobs| {
                    let mut jobs = jobs.borrow_mut();
                    if let Some(j) = jobs.iter_mut().find(|j| j.id == v.id) {
                        *j = v.clone()
                    } else {
                        jobs.insert(0, v.clone());
                    }
                });
                render();
                if done {
                    event_source.close();
                }
            }
        }
    });
    es.set_onmessage(Some(closure.as_ref().unchecked_ref()));
    closure.forget();
}

fn format_speed_str(bytes_per_sec: u64) -> String {
    if bytes_per_sec == 0 {
        return String::new();
    }
    let mut v = bytes_per_sec as f64;
    let units = ["B/s", "KB/s", "MB/s", "GB/s"];
    let mut i = 0;
    while v >= 1024.0 && i < units.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    format!("{v:.1} {}", units[i])
}

fn format_eta_str(seconds: i64) -> String {
    if seconds <= 0 {
        return String::new();
    }
    if seconds < 60 {
        format!("{seconds}s left")
    } else if seconds < 3600 {
        format!("{}m {}s left", seconds / 60, seconds % 60)
    } else {
        format!("{}h {}m left", seconds / 3600, (seconds % 3600) / 60)
    }
}

fn render() {
    let jobs = JOBS.with(|j| j.borrow().clone());
    let current_filter = FILTER.with(|f| f.borrow().clone());

    // Update counts
    let total_count = jobs.len();
    let active_count = jobs
        .iter()
        .filter(|j| ["queued", "downloading", "scanning"].contains(&j.status.as_str()))
        .count();
    let paused_count = jobs.iter().filter(|j| j.status == "paused").count();
    let completed_count = jobs.iter().filter(|j| j.status == "completed").count();

    set_text("job-count", &total_count.to_string());
    set_text("count-all", &total_count.to_string());
    set_text("count-active", &active_count.to_string());
    set_text("count-paused", &paused_count.to_string());
    set_text("count-completed", &completed_count.to_string());

    // Compute aggregate speed
    let total_speed: u64 = jobs
        .iter()
        .filter(|j| j.status == "downloading")
        .map(|j| j.speed)
        .sum();

    if let Some(speed_el) = document().get_element_by_id("total-speed") {
        if total_speed > 0 {
            speed_el.set_text_content(Some(&format!("⚡ {}", format_speed_str(total_speed))));
            let _ = speed_el.class_list().remove_1("hidden");
        } else {
            let _ = speed_el.class_list().add_1("hidden");
        }
    }

    // Filter jobs for display
    let filtered: Vec<&Job> = jobs
        .iter()
        .filter(|j| match current_filter.as_str() {
            "active" => ["queued", "downloading", "scanning"].contains(&j.status.as_str()),
            "paused" => j.status == "paused",
            "completed" => j.status == "completed",
            _ => true,
        })
        .collect();

    let _ = by_id("empty-state")
        .class_list()
        .toggle_with_force("hidden", !filtered.is_empty());

    let html = filtered
        .iter()
        .map(|job| {
            let active = ["queued", "downloading", "scanning"].contains(&job.status.as_str());
            let is_paused = job.status == "paused";

            let mut meta_tags = Vec::new();

            if active {
                if job.size > 0 {
                    meta_tags.push(format!(
                        "<span class=\"telemetry-tag\">{} / {}</span>",
                        size(job.downloaded as i64),
                        size(job.size)
                    ));
                } else if job.downloaded > 0 {
                    meta_tags.push(format!(
                        "<span class=\"telemetry-tag\">{}</span>",
                        size(job.downloaded as i64)
                    ));
                }

                if job.status == "downloading" {
                    if job.speed > 0 {
                        meta_tags.push(format!(
                            "<span class=\"telemetry-tag speed\">⚡ {}</span>",
                            format_speed_str(job.speed)
                        ));
                    }
                    if job.eta > 0 {
                        meta_tags.push(format!(
                            "<span class=\"telemetry-tag eta\">⏳ {}</span>",
                            format_eta_str(job.eta)
                        ));
                    }
                    if !job.segments.is_empty() {
                        let active_segs = job.segments.iter().filter(|s| s.pos < s.end).count();
                        if active_segs > 1 {
                            meta_tags.push(format!(
                                "<span class=\"telemetry-tag conns\">🔗 {active_segs} conns</span>"
                            ));
                        }
                    }
                } else if job.status == "queued" {
                    meta_tags.push("<span class=\"telemetry-tag\">Waiting for slot</span>".into());
                } else if job.status == "scanning" {
                    meta_tags.push("<span class=\"telemetry-tag\">Security scanning…</span>".into());
                }
            } else if is_paused {
                if job.size > 0 {
                    meta_tags.push(format!(
                        "<span class=\"telemetry-tag\">{} / {}</span>",
                        size(job.downloaded as i64),
                        size(job.size)
                    ));
                } else if job.downloaded > 0 {
                    meta_tags.push(format!(
                        "<span class=\"telemetry-tag\">{}</span>",
                        size(job.downloaded as i64)
                    ));
                }
                meta_tags.push("<span class=\"telemetry-tag\">⏸ Paused</span>".into());
            } else if job.status == "completed" {
                let amount = if job.size > 0 {
                    job.size
                } else {
                    job.downloaded as i64
                };
                meta_tags.push(format!(
                    "<span class=\"telemetry-tag\">{}</span>",
                    size(amount)
                ));
                meta_tags.push(format!(
                    "<span class=\"telemetry-tag\">{}</span>",
                    date(job.updated)
                ));
                match job.scan.as_str() {
                    "clean" => meta_tags.push("<span class=\"telemetry-tag\">✓ Clean</span>".into()),
                    "not_scanned" => meta_tags.push("<span class=\"telemetry-tag\">Unscanned</span>".into()),
                    _ => {}
                }
            } else if job.status == "cancelled" {
                meta_tags.push("<span class=\"telemetry-tag\">Cancelled</span>".into());
            } else {
                meta_tags.push(format!(
                    "<span class=\"telemetry-tag\" style=\"color:#ff9e91;\">{}</span>",
                    escape(&job.error)
                ));
            }

            let meta = meta_tags.join(" ");

            let progress = if job.size > 0 {
                (job.downloaded as f64 / job.size as f64 * 100.0).min(100.0)
            } else {
                0.0
            };

            let mut actions = Vec::new();
            if job.status == "completed" {
                actions.push(format!(
                    "<button class=\"job-action\" data-save=\"{}\" title=\"Save file\">↓ Save</button>",
                    escape(&job.id)
                ));
                if !job.url.is_empty() {
                    actions.push(format!(
                        "<button class=\"job-action\" data-copy=\"{}\" title=\"Copy direct link\">⧉ Copy</button>",
                        escape(&job.url)
                    ));
                }
            } else if job.status == "downloading" {
                actions.push(format!(
                    "<button class=\"job-action\" data-pause=\"{}\" title=\"Pause download\">⏸ Pause</button>",
                    escape(&job.id)
                ));
                actions.push(format!(
                    "<button class=\"job-action\" data-cancel=\"{}\" title=\"Cancel download\">✕ Cancel</button>",
                    escape(&job.id)
                ));
            } else if is_paused {
                actions.push(format!(
                    "<button class=\"job-action\" data-resume=\"{}\" title=\"Resume download\">▶ Resume</button>",
                    escape(&job.id)
                ));
                actions.push(format!(
                    "<button class=\"job-action\" data-cancel=\"{}\" title=\"Cancel and remove\">✕ Cancel</button>",
                    escape(&job.id)
                ));
            } else if job.status == "failed" {
                actions.push(format!(
                    "<button class=\"job-action\" data-resume=\"{}\" title=\"Retry download\">↺ Retry</button>",
                    escape(&job.id)
                ));
                actions.push(format!(
                    "<button class=\"job-action\" data-cancel=\"{}\" title=\"Dismiss\">✕ Dismiss</button>",
                    escape(&job.id)
                ));
            } else if active {
                actions.push(format!(
                    "<button class=\"job-action\" data-cancel=\"{}\" title=\"Cancel\">✕ Cancel</button>",
                    escape(&job.id)
                ));
            }

            let action_html = actions.join("");

            let progress_bar = if (active || is_paused) && !job.segments.is_empty() && job.size > 0 {
                let total = job.size as f64;
                let paused_class = if is_paused { " paused" } else { "" };
                let segs_html: String = job
                    .segments
                    .iter()
                    .enumerate()
                    .map(|(i, s)| {
                        let left = (s.start as f64 / total * 100.0).clamp(0.0, 100.0);
                        let part_len = s.end.saturating_sub(s.start);
                        let width = (part_len as f64 / total * 100.0).clamp(0.1, 100.0);
                        let completed = s.pos.saturating_sub(s.start);
                        let fill_pct = if part_len > 0 {
                            (completed as f64 / part_len as f64 * 100.0).clamp(0.0, 100.0)
                        } else {
                            100.0
                        };
                        let part_done = completed;
                        let part_total = part_len;
                        let tip = format!(
                            "Part {}: {} of {} ({:.1}%)",
                            i + 1,
                            size(part_done as i64),
                            size(part_total as i64),
                            fill_pct
                        );
                        format!(
                            "<div class=\"segment-part\" style=\"left:{left:.2}%;width:{width:.2}%;\" title=\"{tip}\"><div class=\"segment-fill{paused_class}\" style=\"width:{fill_pct:.2}%;\"></div></div>"
                        )
                    })
                    .collect();
                format!("<div class=\"segments-bar\">{segs_html}</div>")
            } else if active && job.size <= 0 {
                // Indeterminate progress bar for streaming downloads with unknown size
                "<div class=\"progress-track indeterminate\"><div class=\"progress-fill\"></div></div>".into()
            } else if active || is_paused {
                format!(
                    "<div class=\"progress-track\"><div class=\"progress-fill\" style=\"width:{progress}%\"></div></div>"
                )
            } else {
                String::new()
            };

            format!(
                "<article class=\"job-card\"><div class=\"job-top\"><div class=\"job-file-icon\">↧</div><div class=\"job-main\"><div class=\"job-name\" title=\"{}\">{}</div><div class=\"job-sub\">{}</div></div><span class=\"status {}\">{}</span><div class=\"job-actions\">{action_html}</div></div>{progress_bar}</article>",
                escape(&job.name),
                escape(&job.name),
                meta,
                escape(&job.status),
                escape(&job.status)
            )
        })
        .collect::<String>();
    set_html("job-list", &html);
}

async fn pause_job(id: &str) {
    match api(&format!("/downloads/{id}/pause"), "POST", None).await {
        Ok(_) => toast("Download paused."),
        Err(e) => toast(&e),
    }
}

async fn resume_job(id: &str) {
    match api(&format!("/downloads/{id}/resume"), "POST", None).await {
        Ok(_) => {
            watch(id);
            toast("Download resumed.");
        }
        Err(e) => toast(&e),
    }
}

async fn cancel_job(id: &str) {
    match api(&format!("/downloads/{id}"), "DELETE", None).await {
        Ok(_) => toast("Download cancelled."),
        Err(e) => toast(&e),
    }
}

fn save_job(id: &str) {
    let id = id.to_string();
    if FOLDER.with(|f| f.borrow().is_some()) {
        spawn_local(async move {
            if let Err(e) = save_to_folder(&id).await {
                toast(&e);
                save_browser(&id);
            }
        });
        return;
    }
    save_browser(&id);
}

fn save_browser(id: &str) {
    let Some(anchor) = document()
        .create_element("a")
        .ok()
        .and_then(|e| e.dyn_into::<HtmlAnchorElement>().ok())
    else {
        return;
    };
    anchor.set_href(&format!("/api/v1/downloads/{id}/file"));
    let name = JOBS.with(|jobs| {
        jobs.borrow()
            .iter()
            .find(|j| j.id == id)
            .map(|j| j.name.clone())
            .unwrap_or_else(|| "download".into())
    });
    anchor.set_download(&name);
    anchor.click();
}

fn pick_folder_promise() -> Result<Promise, String> {
    let win = window();
    let win_val: &JsValue = win.as_ref();

    if let Ok(func_val) = reflect_get(win_val, "openFolderPicker") {
        if let Ok(func) = func_val.dyn_into::<Function>() {
            let promise_val = func.call0(win_val).map_err(js_error)?;
            return promise_val.dyn_into::<Promise>().map_err(js_error);
        }
    }

    let picker_val = reflect_get(win_val, "showDirectoryPicker").map_err(js_error)?;
    if picker_val.is_undefined() || picker_val.is_null() {
        return Err("Browser does not support directory selection.".into());
    }
    let picker: Function = picker_val.dyn_into().map_err(js_error)?;
    let promise_val = picker.call0(win_val).map_err(js_error)?;
    promise_val.dyn_into::<Promise>().map_err(js_error)
}

async fn save_to_folder(id: &str) -> Result<(), String> {
    let folder = FOLDER
        .with(|f| f.borrow().clone())
        .ok_or_else(|| "Select a folder first.".to_string())?;

    let job = JOBS
        .with(|jobs| jobs.borrow().iter().find(|j| j.id == id).cloned())
        .ok_or_else(|| "Download not found.".to_string())?;

    let url = format!("/api/v1/downloads/{id}/file");

    let response_val = JsFuture::from(window().fetch_with_str(&url))
        .await
        .map_err(js_error)?;

    let response: Response = response_val
        .dyn_into()
        .map_err(js_error)?;

    if !response.ok() {
        return Err(format!(
            "Could not fetch the file (HTTP {}).",
            response.status()
        ));
    }

    let body = response
        .body()
        .ok_or_else(|| "The server returned an empty response.".to_string())?;

    let folder_val: &JsValue = folder.as_ref();

    let get_file: Function = reflect_get(folder_val, "getFileHandle")
        .map_err(js_error)?
        .dyn_into()
        .map_err(js_error)?;

    let options = Object::new();
    reflect_set(options.as_ref(), "create", &JsValue::TRUE).map_err(js_error)?;

    let handle_promise: Promise = get_file
        .call2(
            folder_val,
            &JsValue::from_str(&job.name),
            options.as_ref(),
        )
        .map_err(js_error)?
        .dyn_into()
        .map_err(js_error)?;

    let handle = JsFuture::from(handle_promise)
        .await
        .map_err(js_error)?;

    let create_writable: Function = reflect_get(&handle, "createWritable")
        .map_err(js_error)?
        .dyn_into()
        .map_err(js_error)?;

    let writable_promise: Promise = create_writable
        .call0(&handle)
        .map_err(js_error)?
        .dyn_into()
        .map_err(js_error)?;

    let writable = JsFuture::from(writable_promise)
        .await
        .map_err(js_error)?;

    let pipe_to: Function = reflect_get(body.as_ref(), "pipeTo")
        .map_err(js_error)?
        .dyn_into()
        .map_err(js_error)?;

    let pipe_promise: Promise = pipe_to
        .call1(body.as_ref(), &writable)
        .map_err(js_error)?
        .dyn_into()
        .map_err(js_error)?;

    JsFuture::from(pipe_promise)
        .await
        .map_err(js_error)?;

    toast("Saved to your selected folder.");
    Ok(())
}

fn reflect_get(target: &JsValue, key: &str) -> Result<JsValue, JsValue> {
    let key_val = JsValue::from_str(key);
    Reflect::get(target, &key_val)
}

fn reflect_set(target: &JsValue, key: &str, value: &JsValue) -> Result<bool, JsValue> {
    let key_val = JsValue::from_str(key);
    Reflect::set(target, &key_val, value)
}

fn prop(value: &JsValue, key: &str) -> String {
    reflect_get(value, key)
        .ok()
        .and_then(|value| value.as_string())
        .unwrap_or_default()
}

fn toast(message: &str) {
    set_text("toast", message);
    let element = by_id("toast");
    let _ = element.class_list().add_1("show");
    let callback = Closure::<dyn FnMut()>::once(move || {
        let _ = element.class_list().remove_1("show");
    });
    let _ = window().set_timeout_with_callback_and_timeout_and_arguments_0(
        callback.as_ref().unchecked_ref(),
        3200,
    );
    callback.forget();
}

fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn size(n: i64) -> String {
    if n < 0 {
        return "Unknown size".into();
    }
    let mut v = n as f64;
    let u = ["B", "KB", "MB", "GB", "TB"];
    let mut i = 0;
    while v >= 1024. && i < 4 {
        v /= 1024.;
        i += 1;
    }
    format!("{v:.1} {}", u[i])
}

fn date(ms: u64) -> String {
    Date::new(&JsValue::from_f64(ms as f64))
        .to_date_string()
        .as_string()
        .unwrap_or_default()
}
