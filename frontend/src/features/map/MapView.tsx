/* The map pane: the Leaflet map with its floating controls.
 *
 * Top right, one column of controls: zoom, reset, layers and the PNG export.
 * Bottom left, the legend, collapsed to a single pill until asked for. The
 * historical layers are fetched once, here; known places and a query's
 * results come in as props from the app.
 */
import "leaflet/dist/leaflet.css";
import { Download, Layers, Map as MapIcon, Minus, Plus, RotateCcw } from "lucide-react";
import { useEffect, useRef, useState, type ReactNode } from "react";
import { getJson, type CityPoint, type ProvinceLabel } from "../../api/client";
import { cx, IconButton, useDismiss } from "../../components/ui";
import { formatInteger } from "../../lib/format";
import {
  DETAIL_ZOOM,
  EpigMap,
  LEGEND_STOPS,
  radiusFor,
  type LayerKey,
  type LayerState,
  type MapPlace,
  type MapTheme,
} from "./epigMap";

interface Props {
  known: MapPlace[] | null;
  /** null: no query, show every known place. */
  results: MapPlace[] | null;
  theme: MapTheme;
  pngHref: string;
  /** The results bar runs along the bottom: the legend then sits above it. */
  bottomBar?: boolean;
  children?: ReactNode;
}

// Listed in stacking order, topmost first; provinces are the base.
const TOGGLES: { key: LayerKey; label: string; hint: string; swatch: string }[] = [
  { key: "search", label: "Search results", hint: "Places matching your query", swatch: "bg-search" },
  { key: "cities", label: "Cities", hint: "Hanson 2016", swatch: "bg-city" },
  { key: "roads", label: "Roads", hint: "Barrington Atlas", swatch: "roads" },
  { key: "all", label: "Known places", hint: "Every findspot in the corpus", swatch: "bg-known" },
  { key: "provinces", label: "Roman provinces", hint: "AD 117, with names", swatch: "provinces" },
];

function Swatch({ kind }: { kind: string }) {
  if (kind === "roads") return <span className="h-[3px] w-3 flex-none rounded-full bg-fg-2/70" />;
  if (kind === "provinces")
    return (
      <span className="size-3 flex-none rounded-[3px] bg-[linear-gradient(135deg,#5b4b8a_0_33%,#4f7a4a_33%_66%,#8a7440_66%)] opacity-80" />
    );
  return <span className={cx("swatch", kind)} />;
}

function ControlGroup({ children }: { children: ReactNode }) {
  return <div className="float-card flex flex-col p-1">{children}</div>;
}

export function MapView({ known, results, theme, pngHref, bottomBar, children }: Props) {
  const element = useRef<HTMLDivElement>(null);
  const [map, setMap] = useState<EpigMap | null>(null);
  const [layers, setLayers] = useState<LayerState | null>(null);
  const [layersOpen, setLayersOpen] = useState(false);
  const [legendOpen, setLegendOpen] = useState(false);
  const layersRef = useRef<HTMLDivElement>(null);
  const legendRef = useRef<HTMLDivElement>(null);
  useDismiss(layersRef, layersOpen, () => setLayersOpen(false));
  useDismiss(legendRef, legendOpen, () => setLegendOpen(false));
  const initialTheme = useRef(theme);

  useEffect(() => {
    const instance = new EpigMap(element.current!, initialTheme.current);
    setMap(instance);
    const unsubscribe = instance.onLayersChange(setLayers);
    // Overlays are optional: a layer that fails to load keeps its toggle off.
    const ignore = () => {};
    getJson<GeoJSON.GeoJsonObject>("/map/layers/provinces").then(
      (d) => instance.setProvinces(d),
      ignore,
    );
    getJson<ProvinceLabel[]>("/map/layers/province-labels").then(
      (d) => instance.setProvinceLabels(d),
      ignore,
    );
    getJson<GeoJSON.GeoJsonObject>("/map/layers/roads").then((d) => instance.setRoads(d), ignore);
    getJson<CityPoint[]>("/map/layers/cities").then((d) => instance.setCities(d), ignore);
    // The pane changes size without a window resize (panels, the drawer).
    const observer = new ResizeObserver(() => instance.invalidateSize());
    observer.observe(element.current!);
    return () => {
      observer.disconnect();
      unsubscribe();
      instance.destroy();
    };
  }, []);

  useEffect(() => {
    map?.setTheme(theme);
  }, [map, theme]);

  useEffect(() => {
    if (!map) return;
    if (results) {
      if (known) map.setKnown(known);
      map.showResults(results);
    } else if (known) {
      map.showKnown(known);
    }
  }, [map, known, results]);

  const activeLayers = layers ? TOGGLES.filter(({ key }) => layers[key]?.on).length : 0;

  return (
    <div className="relative h-full w-full overflow-hidden">
      <div ref={element} className="absolute inset-0" role="region" aria-label="Map of inscription places" />

      {children}

      {/* Controls: one column, top right. */}
      <div className="absolute top-3 right-3 z-[1000] flex flex-col items-end gap-2">
        <ControlGroup>
          <IconButton label="Zoom in" size="sm" onClick={() => map?.zoomIn()}>
            <Plus className="size-4" />
          </IconButton>
          <IconButton label="Zoom out" size="sm" onClick={() => map?.zoomOut()}>
            <Minus className="size-4" />
          </IconButton>
          <div className="mx-1.5 my-0.5 h-px bg-line" />
          <IconButton label="Reset view" size="sm" onClick={() => map?.resetView()}>
            <RotateCcw className="size-[15px]" />
          </IconButton>
        </ControlGroup>

        <div ref={layersRef} className="relative">
          <ControlGroup>
            <IconButton
              label="Map layers"
              size="sm"
              active={layersOpen}
              aria-expanded={layersOpen}
              onClick={() => setLayersOpen((open) => !open)}
            >
              <Layers className="size-4" />
            </IconButton>
          </ControlGroup>
          {activeLayers > 0 && !layersOpen && (
            <span className="pointer-events-none absolute -top-1 -left-1 flex size-4 items-center justify-center rounded-full bg-ink text-[0.6rem] font-semibold text-ink-fg">
              {activeLayers}
            </span>
          )}
          <fieldset
            hidden={!layersOpen}
            className="float-card absolute top-0 right-11 w-64 p-1.5"
          >
            <legend className="sr-only">Map layers</legend>
            <div className="px-2.5 pt-1.5 pb-2 text-[0.7rem] font-semibold tracking-wide text-muted uppercase">
              Layers
            </div>
            {TOGGLES.map(({ key, label, hint, swatch }) => {
              const state = layers?.[key];
              return (
                <label
                  key={key}
                  className={cx(
                    "flex items-center gap-3 rounded-lg px-2.5 py-2 transition-colors",
                    key === "provinces" && "mt-1 border-t border-line pt-2.5",
                    state?.available ? "cursor-pointer hover:bg-surface-2" : "cursor-default opacity-50",
                  )}
                >
                  <Swatch kind={swatch} />
                  <span className="min-w-0 flex-1">
                    <span className="block text-[0.8rem] font-medium text-fg">{label}</span>
                    <span className="block truncate text-[0.7rem] text-muted">{hint}</span>
                  </span>
                  <input
                    type="checkbox"
                    data-map-layer={key}
                    className="peer sr-only"
                    checked={Boolean(state?.on)}
                    disabled={!state?.available}
                    onChange={(event) => map?.setLayerWanted(key, event.target.checked)}
                  />
                  <span
                    aria-hidden="true"
                    className="relative h-[18px] w-8 flex-none rounded-full bg-line-strong transition-colors peer-checked:bg-ink peer-focus-visible:ring-2 peer-focus-visible:ring-accent-soft after:absolute after:top-[2px] after:left-[2px] after:size-[14px] after:rounded-full after:bg-surface after:shadow-sm after:transition-transform peer-checked:after:translate-x-[14px]"
                  />
                </label>
              );
            })}
          </fieldset>
        </div>

        <ControlGroup>
          <a
            href={pngHref}
            download
            aria-label="Download the map as PNG"
            title="Download the map as PNG"
            className="inline-flex size-8 items-center justify-center rounded-lg text-fg-2 transition-colors hover:bg-surface-2 hover:text-fg"
          >
            <Download className="size-4" />
          </a>
        </ControlGroup>
      </div>

      {/* Legend: a pill, bottom left, opened on demand. */}
      <div
        ref={legendRef}
        className={cx(
          "absolute left-3 z-[1002] transition-[bottom] duration-200",
          legendOpen && bottomBar ? "bottom-[4.25rem]" : "bottom-3",
        )}
      >
        {legendOpen ? (
          <div className="float-card w-60 p-3.5" role="dialog" aria-label="Legend">
            <div className="mb-3 flex items-center justify-between">
              <span className="text-[0.7rem] font-semibold tracking-wide text-muted uppercase">Legend</span>
              <button
                type="button"
                className="text-xs font-medium text-muted hover:text-fg"
                aria-label="Hide legend"
                onClick={() => setLegendOpen(false)}
              >
                Hide
              </button>
            </div>
            <div className="grid grid-cols-[12px_1fr] items-center gap-x-2.5 gap-y-1.5 text-[0.8rem] text-fg-2">
              <span className="swatch bg-known" />
              <span>Known places</span>
              <span className="swatch bg-search" />
              <span>Search results</span>
              <span className="swatch bg-city" />
              <span>Cities</span>
            </div>
            <div className="mt-3.5 mb-1.5 text-[0.7rem] font-semibold tracking-wide text-muted uppercase">
              Inscriptions per place
            </div>
            <div className="flex items-end justify-between px-1">
              {LEGEND_STOPS.map((stop) => {
                const diameter = Math.max(6, radiusFor(stop) * 2);
                return (
                  <div key={stop} className="flex flex-col items-center gap-1.5">
                    <span className="flex h-6 items-end">
                      <span
                        className="block rounded-full bg-known ring-1 ring-surface"
                        style={{ width: diameter, height: diameter }}
                      />
                    </span>
                    <span className="text-[0.7rem] text-muted tabular-nums">{formatInteger(stop)}</span>
                  </div>
                );
              })}
            </div>
            <p className="mt-3 text-[0.72rem] leading-relaxed text-muted">
              Below zoom {DETAIL_ZOOM}, known places are grouped: each dot is an area, larger and
              stronger with more inscriptions.
            </p>
          </div>
        ) : (
          <button
            type="button"
            onClick={() => setLegendOpen(true)}
            aria-expanded="false"
            className="float-card flex h-8 items-center gap-2 px-3 text-[0.78rem] font-medium text-fg-2 transition-colors hover:text-fg"
          >
            <MapIcon className="size-3.5" />
            Legend
          </button>
        )}
      </div>
    </div>
  );
}
