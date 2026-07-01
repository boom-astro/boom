/**
 * LLM Filter Agent — converts natural language to the block/condition filter tree.
 * Uses Groq API with Llama 3.3 70B.
 */

import type { FilterBlock, FieldOption } from "./filterSchema";

const GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions";

function buildSystemPrompt(fieldOptions: FieldOption[]): string {
  // Group fields by their group for cleaner presentation
  const grouped = new Map<string, FieldOption[]>();
  for (const opt of fieldOptions) {
    if (!grouped.has(opt.group)) grouped.set(opt.group, []);
    grouped.get(opt.group)!.push(opt);
  }

  let fieldsSection = "";
  for (const [group, fields] of grouped) {
    fieldsSection += `\n### ${group}\n`;
    for (const f of fields) {
      fieldsSection += `- ${f.label} (${f.type})\n`;
    }
  }

  return `You are an astronomical alert filter builder for the Zwicky Transient Facility (ZTF).

Given a natural language description of what alerts a user wants to find, generate a filter in the following JSON format.

## Output Format

Return ONLY a JSON array (no markdown, no explanation). The array contains filter blocks:

[
  {
    "id": "root-block",
    "category": "block",
    "operator": "and",
    "children": [...]
  }
]

Each child is either a **condition**:
{
  "id": "cond-1",
  "category": "condition",
  "field": "candidate.drb",
  "operator": "$gt",
  "value": 0.5
}

Or a **nested block** (for OR logic):
{
  "id": "block-2",
  "category": "block",
  "operator": "or",
  "children": [...]
}

## Available Operators
- "$eq" (equals)
- "$ne" (not equal)
- "$gt" (greater than)
- "$gte" (greater than or equal)
- "$lt" (less than)
- "$lte" (less than or equal)
- "$in" (in list)
- "$exists" (field exists, value should be true/false)

## Available Fields
${fieldsSection}

## Common ZTF Conventions
- candidate.drb: deep-learning real-bogus score (0-1). Higher = more likely real. Use > 0.5 for real alerts.
- candidate.magpsf: PSF magnitude. Brighter objects have LOWER magnitude. "brighter than 18" means < 18.
- candidate.sgscore1: star-galaxy score. 0 = galaxy, 1 = star.
- candidate.jd: Julian Date of observation.
- candidate.ndethist: number of spatially coincident detections. Low values = new/recent.
- candidate.isdiffpos: positive difference image detection (true for real transients).
- classifications.acai_h: ACAI "human-interesting" transient score (0-1). Higher = more likely a real transient interesting to humans. Best indicator for supernovae.
- classifications.acai_n: ACAI "nuclear" transient score (0-1). Higher = near galaxy nucleus (AGN-like).
- classifications.acai_v: ACAI "variable star" score (0-1). Higher = likely variable star.
- classifications.acai_o: ACAI "orphan" transient score (0-1). Higher = hostless/orphan transient.
- classifications.acai_b: ACAI "bogus" score (0-1). Higher = likely bogus/artifact.
- classifications.btsbot: BTS Bot real transient score (0-1). Higher = more likely a real transient.
- properties.rock: boolean. true = likely a solar system object (asteroid).
- properties.star: boolean. true = likely a star, not a transient.
- properties.near_brightstar: boolean. true = near a bright star (higher artifact risk).
- properties.stationary: boolean. true = not moving (non-asteroid).
- coordinates.l: galactic longitude in degrees.
- coordinates.b: galactic latitude in degrees. |b| < 15 = galactic plane (higher stellar contamination).

## Rules
- Always generate unique IDs for each node (use cond-1, cond-2, block-2 etc.)
- Always include at least one condition
- Use "and" as the default operator for the root block
- For "real transients" or "supernovae", include classifications.acai_h > 0.5 AND properties.rock = false AND properties.star = false
- For "bright transients", combine classifications.acai_h > 0.5 with candidate.magpsf < [threshold]
- For filtering out bogus, use classifications.acai_b < 0.5
- For avoiding the galactic plane, use coordinates.b > 15 OR coordinates.b < -15
- Return ONLY the JSON array, nothing else`;
}

export async function generateFilterFromNaturalLanguage(
  query: string,
  fieldOptions: FieldOption[],
  groqApiKey: string,
): Promise<{ filters: FilterBlock[] | null; error: string | null }> {
  if (!groqApiKey) {
    return { filters: null, error: "Groq API key not configured. Set VITE_GROQ_API_KEY in your .env file." };
  }

  try {
    const response = await fetch(GROQ_API_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${groqApiKey}`,
      },
      body: JSON.stringify({
        model: "llama-3.3-70b-versatile",
        messages: [
          { role: "system", content: buildSystemPrompt(fieldOptions) },
          { role: "user", content: query },
        ],
        temperature: 0.1,
        max_tokens: 2048,
      }),
    });

    if (!response.ok) {
      const err = await response.text();
      return { filters: null, error: `Groq API error (${response.status}): ${err}` };
    }

    const data = await response.json();
    const content = data.choices?.[0]?.message?.content?.trim();

    if (!content) {
      return { filters: null, error: "Empty response from LLM." };
    }

    // Try to parse the JSON (handle potential markdown wrapping)
    let jsonStr = content;
    if (jsonStr.startsWith("```")) {
      jsonStr = jsonStr.replace(/^```(?:json)?\n?/, "").replace(/\n?```$/, "");
    }

    const parsed = JSON.parse(jsonStr);

    if (!Array.isArray(parsed) || parsed.length === 0) {
      return { filters: null, error: "LLM returned invalid filter structure." };
    }

    // Basic validation
    const first = parsed[0];
    if (!first.category || !first.id) {
      return { filters: null, error: "LLM output is missing required fields (id, category)." };
    }

    return { filters: parsed as FilterBlock[], error: null };
  } catch (err) {
    const message = err instanceof Error ? err.message : "Unknown error";
    return { filters: null, error: `Failed to generate filter: ${message}` };
  }
}
