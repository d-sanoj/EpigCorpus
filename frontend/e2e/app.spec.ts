import { expect, test, type Page } from "@playwright/test";

// The tests hold for any corpus: they check the shape of what is shown, not
// counts, which change with every harvest.
const COUNT = "[\\d,]+";

// OpenFreeMap is outside the app; tests run without it (the map's overlays
// draw on the plain background).
test.beforeEach(async ({ page }) => {
  await page.route(/tiles\.openfreemap\.org/, (route) => route.abort());
});

async function search(page: Page, keyword: string) {
  await page.getByRole("searchbox", { name: "Keyword" }).fill(keyword);
  await page.getByRole("button", { name: "Search", exact: true }).click();
}

async function choose(page: Page, label: string, value: string) {
  const box = page.getByRole("combobox", { name: label });
  await box.fill(value);
  // An option's name is its value plus any short form under it ("Latin la").
  await page.getByRole("option", { name: new RegExp(`^${value}\\b`) }).first().click();
  await box.blur();
}

async function openLayers(page: Page) {
  await page.getByRole("button", { name: "Map layers" }).click();
}

const layer = (page: Page, key: string) => page.locator(`[data-map-layer="${key}"]`);

test("with no query the map shows every known place", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByText("All known places")).toBeVisible();
  await expect(page.locator("#summary")).toHaveText(new RegExp(`^${COUNT} places · ${COUNT} inscriptions$`));
  await openLayers(page);
  await expect(layer(page, "all")).toBeChecked();
  await expect(layer(page, "search")).toBeDisabled();
  await expect(layer(page, "provinces")).toBeChecked();
  await expect(page.locator(".province-label").first()).toBeAttached();
  await expect(page.getByRole("region", { name: "Results" })).toHaveCount(0);
});

test("the legend starts collapsed and opens on demand", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("dialog", { name: "Legend" })).toHaveCount(0);
  await page.getByRole("button", { name: "Legend" }).click();
  await expect(page.getByRole("dialog", { name: "Legend" })).toBeVisible();
  await page.getByRole("button", { name: "Hide legend" }).click();
  await expect(page.getByRole("dialog", { name: "Legend" })).toHaveCount(0);
});

test("a keyword search fills the map, the drawer and the address bar", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByText("All known places")).toBeVisible();
  await search(page, "viator");

  await expect(page.locator("#summary")).toHaveText(new RegExp(`^${COUNT} inscriptions · ${COUNT} places`));
  await expect(page).toHaveURL(/\?q=viator&search_in=raw$/);

  // The results drawer starts collapsed: a bar with the counts and the TSV.
  const results = page.getByRole("region", { name: "Results" });
  await expect(results.getByRole("table")).toHaveCount(0);
  await expect(results.getByRole("link", { name: /TSV/ })).toHaveAttribute(
    "href",
    "/api/v1/exports/search.tsv?q=viator&search_in=raw",
  );

  await results.getByRole("button", { name: "Show table" }).click();
  const status = results.getByRole("status");
  // The table starts at "Showing 0 of N" while its first page loads.
  await expect(status).toHaveText(new RegExp(`^Showing [1-9][\\d,]* of ${COUNT} inscriptions$`));
  const total = Number((await status.textContent())!.match(/of ([\d,]+)/)![1].replace(/,/g, ""));
  await expect(results.locator("tbody tr")).toHaveCount(Math.min(100, total));
  if (total > 100) {
    await results.getByRole("button", { name: "Show more" }).click();
    await expect(results.locator("tbody tr")).toHaveCount(Math.min(200, total));
  }
  await results.getByRole("button", { name: "Hide table" }).click();
  await expect(results.getByRole("table")).toHaveCount(0);

  // Known places step aside for results, but can be switched back on.
  await openLayers(page);
  await expect(layer(page, "search")).toBeChecked();
  await expect(layer(page, "all")).not.toBeChecked();
  await expect(layer(page, "all")).toBeEnabled();
  const png = page.getByRole("link", { name: "Download the map as PNG" });
  await expect(png).not.toHaveAttribute("href", /layer=known/);
  await page.locator('label:has([data-map-layer="all"])').click();
  await expect(layer(page, "all")).toBeChecked();
  // The PNG download follows the layers switched on.
  await expect(png).toHaveAttribute("href", /q=viator.*layer=provinces.*layer=known.*layer=results/);
});

test("filters alone are a query", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByText("All known places")).toBeVisible();
  await choose(page, "Province", "Noricum");
  await choose(page, "Language", "Latin");
  await page.getByRole("button", { name: "Search", exact: true }).click();
  await expect(page.locator("#summary")).toHaveText(new RegExp(`^${COUNT} inscriptions · ${COUNT} places`));
  await expect(page.getByRole("button", { name: "Remove Noricum" })).toBeVisible();
});

test("a shared address restores the query", async ({ page }) => {
  await page.goto("/?q=viator&search_in=interpretive");
  await expect(page.getByRole("searchbox", { name: "Keyword" })).toHaveValue("viator");
  await expect(page.getByRole("radio", { name: "Interpretive" })).toHaveAttribute("aria-checked", "true");
  await expect(page.locator("#summary")).toHaveText(new RegExp(`^${COUNT} inscriptions`));
  await page.getByRole("button", { name: "Show table" }).click();
  await expect(page.getByRole("columnheader", { name: "Interpretive text" })).toBeVisible();
});

test("nothing matching says so", async ({ page }) => {
  await page.goto("/");
  await search(page, "zzzzqqqq");
  await expect(page.locator("#summary")).toContainText("Nothing matches");
  await expect(page.getByRole("region", { name: "Results" })).toHaveCount(0);
});

test("the theme can be switched and is remembered", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("radio", { name: "Dark" }).click();
  await expect(page.locator("html")).toHaveClass(/dark/);
  await page.reload();
  await expect(page.locator("html")).toHaveClass(/dark/);
  await page.getByRole("radio", { name: "Light" }).click();
  await expect(page.locator("html")).not.toHaveClass(/dark/);
});

test("the pipeline opens in a panel", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Pipeline" }).click();
  const panel = page.getByRole("dialog", { name: "Data pipeline" });
  await expect(panel).toBeVisible();
  await expect(panel.getByRole("button", { name: "Run pipeline" })).toBeEnabled();
  await panel.getByText("Harvest from the EDCS", { exact: true }).click();
  await panel.getByText("Clean and reload the corpus", { exact: true }).click();
  await expect(panel.getByRole("button", { name: "Run pipeline" })).toBeDisabled();
  await page.keyboard.press("Escape");
  await expect(panel).toBeHidden();
});
