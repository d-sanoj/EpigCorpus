import { emptyQuery, filterParams, fromSearch, isQuery, queryParams, toSearch } from "./query";

describe("query", () => {
  it("counts a keyword or any filter as a query", () => {
    expect(isQuery(emptyQuery())).toBe(false);
    expect(isQuery({ ...emptyQuery(), q: "viator" })).toBe(true);
    const filtered = emptyQuery();
    filtered.filters.province = ["Noricum"];
    expect(isQuery(filtered)).toBe(true);
  });

  it("writes repeated filters as repeated parameters", () => {
    const query = emptyQuery();
    query.q = "viator";
    query.filters.province = ["Noricum", "Dalmatia"];
    query.filters.language = ["Latin"];
    expect(queryParams(query, { limit: 5 })).toBe(
      "q=viator&search_in=raw&language=Latin&province=Noricum&province=Dalmatia&limit=5",
    );
    expect(filterParams(query)).toBe("language=Latin&province=Noricum&province=Dalmatia");
  });

  it("round-trips through the address bar", () => {
    const query = fromSearch("?q=vixit&search_in=interpretive&place=Roma&place=Ostia");
    expect(query.q).toBe("vixit");
    expect(query.searchIn).toBe("interpretive");
    expect(query.filters.place).toEqual(["Roma", "Ostia"]);
    expect(fromSearch(toSearch(query))).toEqual(query);
    expect(toSearch(emptyQuery())).toBe("");
  });

  it("falls back to raw text for an unknown search mode", () => {
    expect(fromSearch("?search_in=diplomatic").searchIn).toBe("raw");
  });
});
