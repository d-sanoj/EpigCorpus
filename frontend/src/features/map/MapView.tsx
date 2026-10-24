/* The map pane: the Leaflet map, its layer toggles and its legend.
 *
 * The historical layers are fetched once, here; the known places and a
 * query's results come in as props from the app.
 */
import "leaflet/dist/leaflet.css";
import { useEffect, useRef, useState } from "react";
import { getJson, type Basemap, type CityPoint, type ProvinceLabel } from "../../api/client";
import { formatInteger } from "../../lib/format";
import {
  EpigMap,
  LEGEND_STOPS,
  radiusFor,
  type LayerKey,
  type LayerState,
  type MapPlace,
} from "./epigMap";

interface Props {
  known: MapPlace[] | null;
  /** null: no query, show every known place. */
  results: MapPlace[] | null;
}

// Listed in stacking order, topmost first; provinces are the base.
const TOGGLES: { key: LayerKey; label: string; swatch: string }[] = [
  { key: "search", label: "Search results", swatch: "swatch-search" },
  { key: "cities", label: "Cities", swatch: "swatch-cities" },
  { key: "roads", label: "Roads", swatch: "swatch-roads" },
  { key: "all", label: "Known places", swatch: "swatch-known" },
  { key: "provinces", label: "Roman provinces", swatch: "swatch-provinces" },
];

export function MapView({ known, results }: Props) {
  const element = useRef<HTMLDivElement>(null);
  const [map, setMap] = useState<EpigMap | null>(null);
  const [layers, setLayers] = useState<LayerState | null>(null);
  const [keyMissing, setKeyMissing] = useState(false);

  useEffect(() => {
    const instance = new EpigMap(element.current!);
    setMap(instance);
    const unsubscribe = instance.onLayersChange(setLayers);
    // Overlays are optional: a layer that fails to load keeps its toggle off.
    const ignore = () => {};
    getJson<Basemap>("/map/layers/basemap").then((config) => {
      instance.setBasemap(config);
      setKeyMissing(!config.key_configured);
    }, ignore);
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
    // The pane can change size without a window resize (the table below it).
    const observer = new ResizeObserver(() => instance.invalidateSize());
    observer.observe(element.current!);
    return () => {
      observer.disconnect();
      unsubscribe();
      instance.destroy();
    };
  }, []);

  useEffect(() => {
    if (!map) return;
    if (results) {
      if (known) map.setKnown(known);
      map.showResults(results);
    } else if (known) {
      map.showKnown(known);
    }
  }, [map, known, results]);

  return (
    <div className="relative h-full min-h-80 overflow-hidden rounded-xl border border-slate-800 bg-[#0b0f17]">
      <div
        ref={element}
        className="h-full w-full"
        role="region"
        aria-label="Map of inscription places"
      />

      {keyMissing && (
        <div
          role="alert"
          className="absolute top-2.5 left-14 z-[1000] max-w-md rounded-lg border border-amber-500/50 bg-slate-900/95 px-3 py-2 text-xs text-amber-100"
        >
          CARTO needs an API key for its map tiles. Get a free one at{" "}
          <a className="underline" href="https://carto.com/basemaps/apikey" target="_blank" rel="noreferrer">
            carto.com/basemaps/apikey
          </a>
          , set <code>EPIGCORPUS_CARTO_KEY</code> for the server and restart it.
        </div>
      )}

      <fieldset className="map-panel absolute top-2.5 right-2.5 z-[1000] grid gap-1.5 px-3 py-2.5">
        <legend className="sr-only">Map layers</legend>
        {TOGGLES.map(({ key, label, swatch }) => {
          const state = layers?.[key];
          return (
            <label
              key={key}
              className={`flex items-center gap-1.5 whitespace-nowrap ${
                key === "provinces" ? "mt-1 border-t border-slate-600/40 pt-1.5" : ""
              } ${state?.available ? "cursor-pointer" : "cursor-default text-slate-500"}`}
            >
              <input
                type="checkbox"
                data-map-layer={key}
                className="accent-teal-400"
                checked={Boolean(state?.on)}
                disabled={!state?.available}
                onChange={(event) => map?.setLayerWanted(key, event.target.checked)}
              />
              <span className={`swatch ${swatch} ${state?.available ? "" : "opacity-40"}`} />
              {label}
            </label>
          );
        })}
      </fieldset>

      <details
        open
        className="map-panel group absolute right-2.5 bottom-6 z-[1000] hidden px-3 py-2 leading-snug sm:block"
      >
        <summary className="cursor-pointer font-semibold select-none group-open:mb-1.5">
          Legend
        </summary>
        <div className="mb-2 grid grid-cols-[12px_auto] items-center gap-x-1.5 gap-y-1 text-xs text-slate-300">
          <span className="swatch swatch-known" />
          <span>Known places</span>
          <span className="swatch swatch-search" />
          <span>Search results</span>
          <span className="swatch swatch-cities" />
          <span>Cities</span>
        </div>
        <div className="mb-1 text-xs text-slate-300">Inscriptions per place</div>
        <div className="mb-1 max-w-44 text-[0.7rem] leading-tight text-slate-400">
          Zoomed out, known places are grouped: each dot is an area, darker with more
          inscriptions.
        </div>
        {LEGEND_STOPS.map((stop) => {
          const diameter = Math.max(6, radiusFor(stop) * 2);
          return (
            <div key={stop} className="flex h-6.5 items-center gap-2">
              <span className="flex w-6.5 justify-center">
                <span
                  className="block rounded-full border border-[#0b0f17] bg-teal-400"
                  style={{ width: diameter, height: diameter }}
                />
              </span>
              <span>{formatInteger(stop)}</span>
            </div>
          );
        })}
      </details>
    </div>
  );
}
