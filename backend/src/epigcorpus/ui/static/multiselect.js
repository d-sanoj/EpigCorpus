/* A small multi-select with type-to-search, replacing Streamlit's.
 *
 * Options carry an optional short form (the EDCS Latin token or language
 * code), shown dimmed under the English label and searchable too. Only the
 * first matches are rendered, so the ~22,000 places stay quick to filter.
 */
(function () {
  "use strict";

  var RENDER_LIMIT = 200;
  var counter = 0;

  function MultiSelect(container, options) {
    this.name = container.dataset.filter;
    this.label = container.dataset.label;
    this.values = options.values || [];
    this.shortForms = options.shortForms || {};
    this.placeholder = options.placeholder || "Any";
    this.selected = [];
    this.active = -1;
    this.matches = [];
    this.onChange = options.onChange || function () {};
    this._build(container);
  }

  MultiSelect.prototype._build = function (container) {
    var id = "ms-" + (++counter);
    var label = document.createElement("span");
    label.className = "ms-label";
    label.id = id + "-label";
    label.textContent = this.label;

    var box = document.createElement("div");
    box.className = "ms-box";

    var input = document.createElement("input");
    input.type = "text";
    input.id = id;
    input.setAttribute("role", "combobox");
    input.setAttribute("aria-expanded", "false");
    input.setAttribute("aria-controls", id + "-list");
    input.setAttribute("aria-labelledby", id + "-label");
    input.setAttribute("aria-autocomplete", "list");
    input.placeholder = this.placeholder;
    box.appendChild(input);

    var list = document.createElement("ul");
    list.className = "ms-list";
    list.id = id + "-list";
    list.setAttribute("role", "listbox");
    list.setAttribute("aria-multiselectable", "true");
    list.hidden = true;

    container.appendChild(label);
    container.appendChild(box);
    container.appendChild(list);
    this.box = box;
    this.input = input;
    this.list = list;

    var self = this;
    box.addEventListener("mousedown", function (event) {
      if (event.target === box) {
        event.preventDefault();
        input.focus();
      }
    });
    input.addEventListener("focus", function () { self._open(); });
    input.addEventListener("input", function () { self.active = 0; self._render(); });
    input.addEventListener("blur", function () {
      // Let a click on an option land before the list closes.
      setTimeout(function () { self._close(); }, 120);
    });
    input.addEventListener("keydown", function (event) { self._key(event); });
    list.addEventListener("mousedown", function (event) {
      event.preventDefault();
      var item = event.target.closest("li[data-index]");
      if (item) self._pick(self.matches[Number(item.dataset.index)]);
    });
  };

  MultiSelect.prototype._open = function () {
    this.list.hidden = false;
    this.input.setAttribute("aria-expanded", "true");
    this.active = 0;
    this._render();
  };

  MultiSelect.prototype._close = function () {
    this.list.hidden = true;
    this.input.setAttribute("aria-expanded", "false");
    this.input.value = "";
  };

  MultiSelect.prototype._render = function () {
    var needle = this.input.value.trim().toLowerCase();
    var selected = this.selected;
    var shortForms = this.shortForms;
    this.matches = this.values.filter(function (value) {
      if (selected.indexOf(value) >= 0) return false;
      if (!needle) return true;
      var short = shortForms[value] || "";
      return value.toLowerCase().indexOf(needle) >= 0 || short.toLowerCase().indexOf(needle) >= 0;
    });
    var shown = this.matches.slice(0, RENDER_LIMIT);
    var list = this.list;
    list.textContent = "";
    var self = this;
    shown.forEach(function (value, index) {
      var item = document.createElement("li");
      item.setAttribute("role", "option");
      item.dataset.index = String(index);
      item.id = self.input.id + "-opt-" + index;
      item.setAttribute("aria-selected", index === self.active ? "true" : "false");
      item.appendChild(document.createTextNode(value));
      var short = shortForms[value];
      if (short) {
        var dim = document.createElement("span");
        dim.className = "ms-short";
        dim.textContent = short;
        item.appendChild(dim);
      }
      list.appendChild(item);
    });
    if (!shown.length) {
      var empty = document.createElement("li");
      empty.className = "ms-empty";
      empty.textContent = needle ? "No match" : "Nothing left to choose";
      list.appendChild(empty);
    } else if (this.matches.length > shown.length) {
      var more = document.createElement("li");
      more.className = "ms-more";
      more.textContent = (this.matches.length - shown.length).toLocaleString("en-US") +
        " more — type to narrow";
      list.appendChild(more);
    }
    var current = shown.length ? this.input.id + "-opt-" + this.active : "";
    this.input.setAttribute("aria-activedescendant", current);
  };

  MultiSelect.prototype._key = function (event) {
    var visible = Math.min(this.matches.length, RENDER_LIMIT);
    if (event.key === "ArrowDown") {
      event.preventDefault();
      if (this.list.hidden) this._open();
      this.active = Math.min(visible - 1, this.active + 1);
      this._render();
      this._scrollActive();
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      this.active = Math.max(0, this.active - 1);
      this._render();
      this._scrollActive();
    } else if (event.key === "Enter") {
      // Enter picks an option while the list is open with text typed;
      // otherwise it falls through and submits the search form.
      if (!this.list.hidden && this.input.value.trim() && visible) {
        event.preventDefault();
        this._pick(this.matches[Math.max(0, this.active)]);
      }
    } else if (event.key === "Escape") {
      this._close();
    } else if (event.key === "Backspace" && !this.input.value && this.selected.length) {
      this.remove(this.selected[this.selected.length - 1]);
    }
  };

  MultiSelect.prototype._scrollActive = function () {
    var item = this.list.querySelector('[aria-selected="true"]');
    if (item) item.scrollIntoView({ block: "nearest" });
  };

  MultiSelect.prototype._pick = function (value) {
    if (value === undefined || this.selected.indexOf(value) >= 0) return;
    this.selected.push(value);
    this.input.value = "";
    this._chips();
    this._render();
    this.onChange(this.selected.slice());
  };

  MultiSelect.prototype.remove = function (value) {
    this.selected = this.selected.filter(function (item) { return item !== value; });
    this._chips();
    if (!this.list.hidden) this._render();
    this.onChange(this.selected.slice());
  };

  MultiSelect.prototype._chips = function () {
    var box = this.box;
    box.querySelectorAll(".ms-chip").forEach(function (chip) { chip.remove(); });
    var self = this;
    this.selected.forEach(function (value) {
      var chip = document.createElement("span");
      chip.className = "ms-chip";
      var text = document.createElement("span");
      text.textContent = value;
      text.title = value;
      var close = document.createElement("button");
      close.type = "button";
      close.setAttribute("aria-label", "Remove " + value);
      close.textContent = "×";
      close.addEventListener("click", function () { self.remove(value); });
      chip.appendChild(text);
      chip.appendChild(close);
      box.insertBefore(chip, self.input);
    });
    this.input.placeholder = this.selected.length ? "" : this.placeholder;
  };

  MultiSelect.prototype.value = function () { return this.selected.slice(); };

  MultiSelect.prototype.setValue = function (values) {
    var known = this.values;
    this.selected = (values || []).filter(function (value) { return known.indexOf(value) >= 0; });
    this._chips();
  };

  window.MultiSelect = MultiSelect;
})();
