/* Light, dark, or whatever the system prefers; remembered per browser. */
import { useEffect, useState } from "react";

export type ThemeChoice = "light" | "dark" | "system";
export type Theme = "light" | "dark";

const STORAGE_KEY = "epigcorpus-theme";
const query = () => window.matchMedia?.("(prefers-color-scheme: dark)");

function stored(): ThemeChoice {
  try {
    const value = localStorage.getItem(STORAGE_KEY);
    if (value === "light" || value === "dark" || value === "system") return value;
  } catch {
    // Storage blocked (private window): fall back to the default.
  }
  return "system";
}

function resolve(choice: ThemeChoice): Theme {
  if (choice !== "system") return choice;
  return query()?.matches ? "dark" : "light";
}

/** Apply a theme to <html>, cross-fading colours rather than flashing. */
function apply(theme: Theme, animate: boolean) {
  const root = document.documentElement;
  if (animate) root.classList.add("theme-transition");
  root.classList.toggle("dark", theme === "dark");
  if (animate) window.setTimeout(() => root.classList.remove("theme-transition"), 250);
}

export function useTheme(): [Theme, ThemeChoice, (choice: ThemeChoice) => void] {
  const [choice, setChoice] = useState<ThemeChoice>(stored);
  const [theme, setTheme] = useState<Theme>(() => resolve(stored()));

  useEffect(() => {
    const next = resolve(choice);
    setTheme(next);
    apply(next, true);
    try {
      localStorage.setItem(STORAGE_KEY, choice);
    } catch {
      // Not remembered; the choice still applies to this visit.
    }
    if (choice !== "system") return;
    const media = query();
    const follow = () => {
      const followed = resolve("system");
      setTheme(followed);
      apply(followed, true);
    };
    media?.addEventListener("change", follow);
    return () => media?.removeEventListener("change", follow);
  }, [choice]);

  return [theme, choice, setChoice];
}

/** Set the theme before React renders, so the first paint is already right. */
export function applyStoredTheme() {
  apply(resolve(stored()), false);
}
