import json
import re
import urllib.request
from pathlib import Path

REGISTRY = "https://rubin-alert-schemas.slac.stanford.edu/schema-registry/subjects/alert-packet/versions/latest"
SCHEMAS = "https://raw.githubusercontent.com/lsst/alert_packet/main/python/lsst/alert/packet/schema"
SOURCE = Path(__file__).resolve().parent.parent / "src" / "alert" / "lsst.rs"
RECORDS = {
    "DiaSource": "diaSource",
    "DiaObject": "diaObject",
    "DiaForcedSource": "diaForcedSource",
    "SsSource": "ssSource",
}

STRUCT = re.compile(r"^pub struct (\w+)")
FIELD = re.compile(r"^(\s*)pub (\w+):")
RENAME = re.compile(r'rename\s*(?:=\s*"([^"]+)"|\([^)]*deserialize\s*=\s*"([^"]+)")')


def fetch_json(url):
    with urllib.request.urlopen(url, timeout=60) as response:
        return json.load(response)


def rubin_docs():
    version = fetch_json(REGISTRY)["version"]
    major, minor = divmod(version, 100)
    docs = {}
    for record in RECORDS.values():
        schema = fetch_json(
            f"{SCHEMAS}/{major}/{minor}/lsst.v{major}_{minor}.{record}.avsc"
        )
        docs[record] = {
            field["name"]: " ".join(field["doc"].split())
            for field in schema["fields"]
            if field.get("doc", "").strip()
        }
    return f"v{major}.{minor}", docs


def sync(lines, docs):
    out = []
    block = []
    record = None
    updated = 0
    for line in lines:
        struct = STRUCT.match(line)
        if struct:
            record = RECORDS.get(struct.group(1))
        elif line.startswith("}"):
            record = None
        stripped = line.strip()
        if record and (stripped.startswith("///") or stripped.startswith("#[")):
            block.append(line)
            continue
        field = FIELD.match(line) if record else None
        if field:
            indent, name = field.groups()
            attrs = [entry for entry in block if not entry.strip().startswith("///")]
            for attr in attrs:
                rename = RENAME.search(attr)
                if rename:
                    name = rename.group(1) or rename.group(2)
            doc = docs[record].get(name)
            if doc:
                new_block = [f"{indent}/// {doc}\n", *attrs]
                updated += new_block != block
                block = new_block
        out.extend(block)
        block = []
        out.append(line)
    out.extend(block)
    return out, updated


def main():
    version, docs = rubin_docs()
    lines = SOURCE.read_text().splitlines(keepends=True)
    synced, updated = sync(lines, docs)
    if updated:
        SOURCE.write_text("".join(synced))
    print(f"{updated} field docs updated from Rubin's alert schema {version}")


if __name__ == "__main__":
    main()
