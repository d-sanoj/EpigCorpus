import { expect, test, type Page } from "@playwright/test";

// Counts from the committed corpus snapshot (see backend/README.md).
async function search(page: Page, keyword: string) {
  await page.getByLabel("Keyword").fill(keyword);
  await page.getByRole("button", { name: "Search", exact: true }).click();
}

async function choose(page: Page, label: string, value: string) {
  const box = page.getByRole("combobox", { name: label });
  await box.fill(value);
  // An option's name is its value plus any short form under it ("Latin la").
  await page.getByRole("option", { name: new RegExp(`^${value}\\b`) }).first().click();
  await box.blur();
}

test("with no query the map shows every known place", async ({ page }) => {
  await page.goto("/");
  await expect(page.locator("#summary")).toContainText("All known places: 21,999");
  await expect(page.locator('[data-map-layer="all"]')).toBeChecked();
  await expect(page.locator('[data-map-layer="search"]')).toBeDisabled();
  await expect(page.locator('[data-map-layer="provinces"]')).toBeChecked();
  await expect(page.locator(".province-label").first()).toBeAttached();
  await expect(page.getByRole("region", { name: "Results" })).toHaveCount(0);
});

test("a keyword search fills the map, the table and the address bar", async ({ page }) => {
  await page.goto("/");
  await expect(page.locator("#summary")).toContainText("All known places");
  await search(page, "viator");

  await expect(page.locator("#summary")).toContainText("515 inscriptions match");
  await expect(page).toHaveURL(/\?q=viator&search_in=raw$/);
  const results = page.getByRole("region", { name: "Results" });
  await expect(results.getByRole("status")).toHaveText("Showing 100 of 515 inscriptions");
  await results.getByRole("button", { name: "Show more" }).click();
  await expect(results.getByRole("status")).toHaveText("Showing 200 of 515 inscriptions");

  await expect(page.locator('[data-map-layer="search"]')).toBeChecked();
  // Known places step aside for results, but can be switched back on.
  const known = page.locator('[data-map-layer="all"]');
  await expect(known).not.toBeChecked();
  await expect(known).toBeEnabled();
  await known.check();
  await expect(known).toBeChecked();

  const tsv = page.getByRole("link", { name: "Results as TSV" });
  await expect(tsv).toHaveAttribute("href", "/api/v1/exports/search.tsv?q=viator&search_in=raw");
});

test("filters alone are a query", async ({ page }) => {
  await page.goto("/");
  await expect(page.locator("#summary")).toContainText("All known places");
  await choose(page, "Province", "Noricum");
  await choose(page, "Language", "Latin");
  await page.getByRole("button", { name: "Search", exact: true }).click();
  await expect(page.locator("#summary")).toContainText("4,606 inscriptions match");
  await expect(page.getByRole("button", { name: "Remove Noricum" })).toBeVisible();
});

test("a shared address restores the query", async ({ page }) => {
  await page.goto("/?q=viator&search_in=interpretive");
  await expect(page.getByLabel("Keyword")).toHaveValue("viator");
  await expect(page.getByLabel("Search in")).toHaveValue("interpretive");
  await expect(page.locator("#summary")).toContainText("700 inscriptions match");
  await expect(page.getByRole("columnheader", { name: "Interpretive text" })).toBeVisible();
});

test("nothing matching says so", async ({ page }) => {
  await page.goto("/");
  await search(page, "zzzzqqqq");
  await expect(page.locator("#summary")).toContainText("0 inscriptions match");
  await expect(page.getByRole("status").filter({ hasText: "Nothing matches" })).toBeVisible();
});

test("the pipeline panel shows its options", async ({ page }) => {
  await page.goto("/");
  const panel = page.getByRole("region", { name: "Pipeline" });
  await expect(panel.getByRole("button", { name: "Run pipeline" })).toBeEnabled();
  await panel.getByLabel("Harvest from the EDCS").uncheck();
  await panel.getByLabel("Clean and reload the corpus").uncheck();
  await expect(panel.getByRole("button", { name: "Run pipeline" })).toBeDisabled();
});
