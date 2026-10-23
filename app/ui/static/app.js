/* EpigCorpus web UI: the controls, the summary, the map and the results,
 * all driven by the HTTP API under /v1 (see app/api/openapi.yaml).
 *
 * Behaviour carried over from the Streamlit app:
 * - a query is a keyword, any filter, or both; nothing runs until Search is
 *   pressed (or Enter in the keyword box);
 * - with no query the map shows every known place in teal; with one, only the
 *   results in tangerine, and the view fits them;
 * - "Download as PNG" always reflects what the map shows;
 * - the results table and "Download TSV" appear for a query with matches.
 * New: the submitted query is kept in the address bar, so it can be shared.
 */
(function () {
  "use strict";

  var API = "/v1";
  var FILTERS = ["language", "province", "place", "category", "material"];
  var MODE_COLUMNS = {
    raw: { field: "inscription_text", heading: "Raw inscription" },
    interpretive: { field: "inscription_text_interpretive", heading: "Interpretive text" },
    conservative: { field: "inscription_text_conservative", heading: "Conservative text" },
  };
  var TABLE_PAGE = 100;
  var MAX_PLACES = 25000;

  var els = {
    form: document.getElementById("search-form"),
    mode: document.getElementById("search-in"),
    keyword: document.getElementById("keyword"),
    button: document.getElementById("search-button"),
    png: document.getElementById("png-download"),
    summary: document.getElementById("summary"),
    map: document.getElementById("map"),
    notice: document.getElementById("notice"),
    results: document.getElementById("results"),
    tsv: document.getElementById("tsv-download"),
    tbody: document.querySelector("#results-table tbody"),
    textColumn: document.getElementById("text-column"),
    count: document.getElementById("results-count"),
    more: document.getElementById("more-button"),
  };

  var selects = {};
  var map = null;
  var knownPlaces = null;
  var run = 0;
  var tableCursor = null;
  var tableShown = 0;
  var tableTotal = 0;
  var tableQuery = null;

  // ─── helpers ───────────────────────────────────────────────────────────────

  function formatInteger(value) { return Number(value).toLocaleString("en-US"); }

  function getJson(path) {
    return fetch(API + path, { headers: { Accept: "application/json" } }).then(function (response) {
      return response.json().catch(function () { return {}; }).then(function (body) {
        if (!response.ok) {
          var error = new Error(body.detail || body.title || "Request failed (" + response.status + ")");
          error.problem = body;
          throw error;
        }
        return body;
      });
    });
  }

  function queryString(query, extra) {
    var params = new URLSearchParams();
    if (query.q) params.append("q", query.q);
    params.append("search_in", query.searchIn);
    FILTERS.forEach(function (name) {
      query.filters[name].forEach(function (value) { params.append(name, value); });
    });
    Object.keys(extra || {}).forEach(function (key) { params.append(key, extra[key]); });
    return params.toString();
  }

  function isQuery(query) {
    return Boolean(query.q) || FILTERS.some(function (name) { return query.filters[name].length; });
  }

  function notice(text, kind) {
    els.notice.hidden = !text;
    els.notice.className = "notice" + (kind ? " " + kind : "");
    els.notice.textContent = text || "";
  }

  function busy(on) {
    els.button.disabled = on;
    els.button.textContent = on ? "Searching…" : "Search";
  }

  // ─── the query: read from the controls, kept in the address bar ────────────

  function readControls() {
    var filters = {};
    FILTERS.forEach(function (name) { filters[name] = selects[name] ? selects[name].value() : []; });
    return { q: els.keyword.value.trim(), searchIn: els.mode.value, filters: filters };
  }

  function readAddress() {
    var params = new URLSearchParams(window.location.search);
    var filters = {};
    FILTERS.forEach(function (name) { filters[name] = params.getAll(name); });
    var searchIn = params.get("search_in");
    return {
      q: (params.get("q") || "").trim(),
      searchIn: MODE_COLUMNS[searchIn] ? searchIn : "raw",
      filters: filters,
    };
  }

  function writeControls(query) {
    els.keyword.value = query.q;
    els.mode.value = query.searchIn;
    FILTERS.forEach(function (name) { if (selects[name]) selects[name].setValue(query.filters[name]); });
  }

  function remember(query) {
    var search = isQuery(query) ? "?" + queryString(query) : "";
    var url = window.location.pathname + search;
    if (url !== window.location.pathname + window.location.search) {
      window.history.replaceState(null, "", url);
    }
  }

  // ─── running a query ───────────────────────────────────────────────────────

  function submit(query) {
    var mine = ++run;
    remember(query);
    els.png.href = API + "/exports/map.png?" + queryString(query);
    busy(true);
    notice("");

    if (!isQuery(query)) {
      showEverything(mine);
      return;
    }

    els.summary.textContent = "Searching inscriptions…";
    getJson("/places?" + queryString(query, { limit: MAX_PLACES }))
      .then(function (body) {
        if (mine !== run) return;
        var summary = body.summary;
        els.summary.textContent =
          formatInteger(summary.inscriptions) + " inscriptions match. " + summary.subtitle + ".";
        if (map) map.showResults(body.data);
        if (!summary.inscriptions) {
          els.results.hidden = true;
          notice("Nothing matches that combination. Try removing a filter.", "warning");
          return;
        }
        startTable(query, summary.inscriptions);
      })
      .catch(function (error) { if (mine === run) failed(error); })
      .then(function () { if (mine === run) busy(false); });
  }

  // Every known place, fetched once. It backs the no-query view and the
  // "Known places" layer a user can switch on under a query's results.
  function loadKnown() {
    if (!knownPlaces) {
      knownPlaces = getJson("/places/known?limit=" + MAX_PLACES);
      knownPlaces
        .then(function (body) { if (map) map.setKnown(body.data); })
        .catch(function () { knownPlaces = null; });
    }
    return knownPlaces;
  }

  function showEverything(mine) {
    els.results.hidden = true;
    loadKnown()
      .then(function (body) {
        if (mine !== run) return;
        els.summary.textContent =
          "All known places: " + formatInteger(body.summary.places) +
          ". Search by keyword, by filter, or by both.";
        if (map) map.showKnown(body.data);
        notice("Enter a keyword, choose filters, or both, then press Search.");
      })
      .catch(function (error) { if (mine === run) failed(error); })
      .then(function () { if (mine === run) busy(false); });
  }

  function failed(error) {
    els.summary.textContent = "";
    notice(error.message, "error");
  }

  // ─── results table ─────────────────────────────────────────────────────────

  function startTable(query, total) {
    tableQuery = query;
    tableCursor = null;
    tableShown = 0;
    tableTotal = total;
    els.tbody.textContent = "";
    els.textColumn.textContent = MODE_COLUMNS[query.searchIn].heading;
    els.tsv.href = API + "/exports/search.tsv?" + queryString(query);
    els.results.hidden = false;
    loadRows(run);
  }

  function loadRows(mine) {
    var query = tableQuery;
    var path = query.q ? "/inscriptions/search?" : "/inscriptions?";
    var extra = { limit: TABLE_PAGE };
    if (tableCursor) extra.cursor = tableCursor;
    var params = query.q ? queryString(query, extra) : filterString(query, extra);
    els.more.disabled = true;
    getJson(path + params)
      .then(function (body) {
        if (mine !== run) return;
        body.data.forEach(function (row) { els.tbody.appendChild(rowElement(row, query.searchIn)); });
        tableShown += body.data.length;
        tableCursor = body.page.next_cursor;
        els.count.textContent =
          "Showing " + formatInteger(tableShown) + " of " + formatInteger(tableTotal) + " inscriptions";
        els.more.hidden = !tableCursor;
      })
      .catch(function (error) { if (mine === run) notice(error.message, "error"); })
      .then(function () { els.more.disabled = false; });
  }

  // /inscriptions takes only the filters (no q, no search_in).
  function filterString(query, extra) {
    var params = new URLSearchParams();
    FILTERS.forEach(function (name) {
      query.filters[name].forEach(function (value) { params.append(name, value); });
    });
    Object.keys(extra).forEach(function (key) { params.append(key, extra[key]); });
    return params.toString();
  }

  function cell(text, className) {
    var td = document.createElement("td");
    if (className) td.className = className;
    td.textContent = text === null || text === undefined ? "" : String(text);
    return td;
  }

  function dating(row) {
    var from = row.not_before;
    var to = row.not_after;
    if (from === null && to === null) return "";
    if (from === to) return String(from);
    return (from === null ? "?" : from) + " – " + (to === null ? "?" : to);
  }

  function rowElement(row, searchIn) {
    var tr = document.createElement("tr");
    tr.appendChild(cell(row.record_id, "record"));
    tr.appendChild(cell(row.province));
    tr.appendChild(cell(row.place));
    tr.appendChild(cell(dating(row)));
    tr.appendChild(cell(row.language));
    tr.appendChild(cell(row.material_en));
    tr.appendChild(cell((row.category_en || []).join(", ")));
    tr.appendChild(cell(row[MODE_COLUMNS[searchIn].field], "text"));
    return tr;
  }

  // ─── start-up ──────────────────────────────────────────────────────────────

  function buildFilters(facets) {
    document.querySelectorAll("[data-filter]").forEach(function (container) {
      var name = container.dataset.filter;
      var values = facets.values[name] || [];
      selects[name] = new window.MultiSelect(container, {
        values: values,
        shortForms: (facets.short_forms || {})[name] || {},
        placeholder: name === "place"
          ? "Any (" + formatInteger(values.length) + " - type to search)"
          : "Any",
      });
    });
  }

  function loadOverlays() {
    if (!map) return;
    [
      ["provinces", "/map/layers/provinces"],
      ["provinceLabels", "/map/layers/province-labels"],
      ["roads", "/map/layers/roads"],
      ["cities", "/map/layers/cities"],
    ].forEach(function (pair) {
      getJson(pair[1])
        .then(function (data) { map.setOverlay(pair[0], data); })
        .catch(function () { /* overlays are optional; their toggles stay disabled */ });
    });
  }

  function loadLand() {
    if (!map) return;
    getJson("/map/layers/land")
      .then(function (data) { map.setLand(data); })
      .catch(function () { /* without land the markers still draw on the dark sea */ });
  }

  function start() {
    map = window.EpigMap.create(els.map);
    loadLand();
    busy(true);
    getJson("/facets")
      .then(function (facets) {
        buildFilters(facets);
        var query = readAddress();
        writeControls(query);
        submit(query);
        loadKnown();
        loadOverlays();
      })
      .catch(function (error) {
        busy(false);
        failed(error);
      });

    els.form.addEventListener("submit", function (event) {
      event.preventDefault();
      submit(readControls());
    });
    els.more.addEventListener("click", function () { loadRows(run); });
  }

  start();
})();
