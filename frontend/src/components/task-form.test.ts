import { describe, expect, it } from "vitest";
import { baseType, isStructured, parseField, unwrapOptional } from "./task-form";

const value = (...args: Parameters<typeof parseField>) => parseField(...args).value;
const error = (...args: Parameters<typeof parseField>) => parseField(...args).error;

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

describe("unwrapOptional", () => {
  it("resolves the oneOf form of an optional field", () => {
    // utoipa writes Option<T> as a type pair for some T and as a two-arm oneOf
    // for others. Treating every oneOf as a tagged union sent the whole form to
    // the JSON escape hatch over one optional parameter.
    const field = unwrapOptional({
      description: "which survey",
      oneOf: [{ type: "string", enum: ["ztf", "winter"] }, { type: "null" }],
    });
    expect(field.enum).toEqual(["ztf", "winter"]);
    // The doc comment is written on the field, not on the arm.
    expect(field.description).toBe("which survey");
  });

  it("leaves a real tagged union alone", () => {
    const union = { oneOf: [{ type: "object" }, { type: "object" }] };
    expect(unwrapOptional(union)).toBe(union);
  });
});

describe("isStructured", () => {
  it("sends a field the schema only references to the JSON box", () => {
    // `schema_of` serializes one type and keeps no definitions registry, so a
    // struct or enum parameter arrives as a $ref that cannot be followed. It
    // carries no `type` either, so it used to render as a text box and quietly
    // accept a string for a parameter that is not one.
    expect(isStructured({ $ref: "#/components/schemas/Selection" })).toBe(true);
    expect(isStructured({ oneOf: [{ $ref: "#/a" }, { type: "null" }] })).toBe(true);
    expect(isStructured({ type: "object" })).toBe(true);
  });

  it("leaves flat fields, including optional ones, on the form", () => {
    expect(isStructured({ type: "string" })).toBe(false);
    expect(isStructured({ type: ["integer", "null"] })).toBe(false);
    expect(isStructured({ oneOf: [{ type: "string" }, { type: "null" }] })).toBe(false);
  });
});

describe("parseField", () => {
  it("omits a blank field so the server applies its default", () => {
    // The critical case: an empty numeric input must not become 0, which
    // validate_params rejects for batch_size and num_workers.
    expect(parseField({ type: "integer" }, "")).toEqual({});
    expect(parseField({ type: "integer" }, "   ")).toEqual({});
    expect(parseField({ type: "string" }, "")).toEqual({});
  });

  it("parses numbers as numbers, not strings", () => {
    // serde would reject "5000" for a usize field.
    expect(value({ type: "integer" }, "5000")).toBe(5000);
    expect(value({ type: ["integer", "null"] }, "12")).toBe(12);
    expect(value({ type: "number" }, "2460000.5")).toBe(2460000.5);
  });

  it("refuses numeric text instead of silently truncating it", () => {
    // parseInt took the leading digits and discarded the rest, and the result
    // was never NaN, so the check for NaN passed: 1e9 became 1 and 2.5 became
    // 2. An ingest asked for with a batch size of 1e9 ran with a batch size of
    // 1 and reported nothing wrong.
    expect(value({ type: "integer" }, "2.5")).toBeUndefined();
    expect(error({ type: "integer" }, "2.5")).toBe("whole numbers only");
    expect(error({ type: "integer" }, "1,000")).toBe("not a number");
    expect(error({ type: "number" }, "-")).toBe("not a number");
    expect(error({ type: "number" }, "abc")).toBe("not a number");
    expect(error({ type: "number" }, "Infinity")).toBe("not a number");
    // An exponent is a number, and a large one is the server's call to refuse.
    expect(value({ type: "integer" }, "1e9")).toBe(1_000_000_000);
  });

  it("applies the bounds the schema carries", () => {
    // They come from the struct the server validates against, so a value
    // outside them is a 400 waiting to happen.
    const field = { type: "integer", minimum: 1, maximum: 16 };
    expect(value(field, "4")).toBe(4);
    expect(error(field, "0")).toBe("at least 1");
    expect(error(field, "17")).toBe("at most 16");
  });

  it("keeps booleans as booleans, including false", () => {
    // `false` is meaningful — it is not the same as leaving the field unset.
    expect(value({ type: "boolean" }, false)).toBe(false);
    expect(value({ type: "boolean" }, true)).toBe(true);
  });

  it("splits a comma-separated list and drops the gaps", () => {
    // How a catalog list reads most naturally; a trailing comma should not
    // become an empty catalog name the API then rejects.
    expect(value({ type: "array" }, "NED, Gaia_DR3 ,")).toEqual([
      "NED",
      "Gaia_DR3",
    ]);
  });

  it("coerces a list of numbers per entry", () => {
    // Sent as strings, serde rejects the whole list over the first element.
    const field = { type: "array", items: { type: "integer" } };
    expect(value(field, "1, 2, 3")).toEqual([1, 2, 3]);
    expect(error(field, "1, two")).toBe("two: not a number");
  });

  it("trims surrounding whitespace from a string", () => {
    expect(value({ type: "string" }, "  2mass  ")).toBe("2mass");
  });
});
