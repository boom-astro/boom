import { describe, expect, it } from "vitest";
import { baseType, coerce } from "./task-form";

describe("baseType", () => {
  it("ignores the null that marks a field optional", () => {
    // utoipa renders Option<usize> as ["integer", "null"]; rendering that as a
    // text box because the type is an array would be wrong.
    expect(baseType({ type: ["integer", "null"] })).toBe("integer");
    expect(baseType({ type: "boolean" })).toBe("boolean");
  });

  it("falls back to string for an undescribed field", () => {
    expect(baseType({})).toBe("string");
  });
});

describe("coerce", () => {
  it("omits a blank field so the server applies its default", () => {
    // The critical case: an empty numeric input must not become 0, which
    // validate_params rejects for batch_size, processes and n_workers.
    expect(coerce({ type: "integer" }, "")).toBeUndefined();
    expect(coerce({ type: "integer" }, "   ")).toBeUndefined();
    expect(coerce({ type: "string" }, "")).toBeUndefined();
  });

  it("parses numbers as numbers, not strings", () => {
    // serde would reject "5000" for a usize field.
    expect(coerce({ type: "integer" }, "5000")).toBe(5000);
    expect(coerce({ type: ["integer", "null"] }, "12")).toBe(12);
    expect(coerce({ type: "number" }, "2460000.5")).toBe(2460000.5);
  });

  it("keeps booleans as booleans, including false", () => {
    // `false` is meaningful — it is not the same as leaving the field unset.
    expect(coerce({ type: "boolean" }, false)).toBe(false);
    expect(coerce({ type: "boolean" }, true)).toBe(true);
  });

  it("splits a comma-separated list and drops the gaps", () => {
    // How a catalog list reads most naturally; a trailing comma should not
    // become an empty catalog name the API then rejects.
    expect(coerce({ type: "array" }, "NED, Gaia_DR3 ,")).toEqual([
      "NED",
      "Gaia_DR3",
    ]);
  });

  it("trims surrounding whitespace from a string", () => {
    expect(coerce({ type: "string" }, "  2mass  ")).toBe("2mass");
  });
});
