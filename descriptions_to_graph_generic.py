# descriptions_to_graph_generic.py
"""
Excel (one description column) -> Neo4j graph using client schema.

Input: descriptions_only.xlsx
  - Must have a column called 'description'.
  - Each row describes ONE person in free text, like:
      NAME: John Doe; DOB: 1980-01-01; Citizenship: ...; ...

Config: relation_rules.yml
  - field_patterns.person / field_patterns.organization: regex patterns
  - location_relationships: mapping of person field -> relationship type

Graph model:

  (:Record {record_id, description, row_index, source_file})
  (:Entity:Person {
      name, dob, citizenship, place_of_birth, phone_number,
      address, passport_number, flight_number, ...
   })
  (:Entity:Organization {name, address})
  (:Entity:Location {name})

  (:Record)-[:DESCRIBES]->(:Person)

  (:Person)-[:ASSOCIATED_WITH_ORG]->(:Organization)    (if org found)
  (:Person)-[:BORN_IN]->(:Location)                    (place_of_birth)
  (:Person)-[:DEPARTED_FROM]->(:Location)              (departure_location)
  (:Person)-[:ARRIVED_AT]->(:Location)                 (arrival_location)
  (:Person)-[:ARRESTED_AT]->(:Location)                (arrest_location)
"""

from neo4j import GraphDatabase
from pathlib import Path
import pandas as pd
import yaml
import re
from typing import Optional, Dict, Any, List

# --- BASIC CONFIG ---
NEO4J_URI  = "neo4j://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "neo4j123"

EXCEL_PATH  = Path("descriptions_only.xlsx")
RULES_PATH  = Path("relation_rules.yml")
SOURCE_NAME = EXCEL_PATH.name


# --- HELPERS ---
def clean_str(v) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    s = str(v).strip()
    return s if s else None


def load_rules(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing rules file: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


rules = load_rules(RULES_PATH)

RECORD_LABEL = rules.get("record", {}).get("label", "Record")
RECORD_ID_PREFIX = rules.get("record", {}).get("id_prefix", "DESC_")
ENTITY_CONFIG = rules.get("entities", {})
FIELD_PATTERNS = rules.get("field_patterns", {})
LOCATION_REL_MAP = rules.get("location_relationships", {})


# --- NEO4J DRIVER ---
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))


def write(cypher: str, params: Optional[dict] = None):
    with driver.session() as s:
        s.run(cypher, **(params or {}))


def read(cypher: str, params: Optional[dict] = None):
    with driver.session() as s:
        return s.run(cypher, **(params or {})).data()


# --- SCHEMA ---
def ensure_schema():
    # Records
    write(f"""
    CREATE CONSTRAINT IF NOT EXISTS
    FOR (r:{RECORD_LABEL})
    REQUIRE r.record_id IS UNIQUE
    """)

    # Entity uniqueness by (entity_type, canonical_text)
    write("""
    CREATE CONSTRAINT IF NOT EXISTS
    FOR (e:Entity)
    REQUIRE (e.entity_type, e.canonical_text) IS UNIQUE
    """)


# --- EXCEL LOADER ---
def load_descriptions(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Excel file not found: {path}")
    df = pd.read_excel(path)
    if "description" not in df.columns:
        raise ValueError("Excel must contain a 'description' column.")
    df = df.dropna(subset=["description"])
    print(f"Loaded {len(df)} descriptions from {path}")
    return df


# --- FIELD EXTRACTION FROM TEXT ---
def extract_fields(text: str) -> Dict[str, Dict[str, str]]:
    """
    Apply regex patterns from YAML to a description.
    Returns dict:
      {
        "person": {field -> value, ...},
        "organization": {field -> value, ...}
      }
    """
    result = {"person": {}, "organization": {}}

    for group_name, group_cfg in FIELD_PATTERNS.items():
        group_out: Dict[str, str] = {}
        for field_name, field_cfg in group_cfg.items():
            patterns: List[str] = field_cfg.get("patterns", [])
            value_found: Optional[str] = None
            for pat in patterns:
                m = re.search(pat, text, flags=re.IGNORECASE)
                if m:
                    raw = m.group("value")
                    val = raw.strip().strip('",; ')
                    if val:
                        value_found = val
                        break
            if value_found:
                group_out[field_name] = value_found
        result[group_name] = group_out

    return result


# --- GRAPH UPSERTS ---
def upsert_record(record_id: str, row_index: int, description: str):
    params = {
        "record_id": record_id,
        "row_index": row_index,
        "description": description,
        "source_file": SOURCE_NAME,
    }
    cypher = f"""
    MERGE (r:{RECORD_LABEL} {{record_id: $record_id}})
    ON CREATE SET
      r.description = $description,
      r.row_index   = $row_index,
      r.source_file = $source_file,
      r.created_at  = datetime(),
      r.updated_at  = datetime()
    ON MATCH SET
      r.description = $description,
      r.updated_at  = datetime()
    """
    write(cypher, params)


def upsert_person(person_fields: Dict[str, str]) -> Optional[str]:
    """
    Create/merge a Person entity node based on extracted fields.
    Returns the person name (used as canonical_text) or None.
    """
    name = clean_str(person_fields.get("name"))
    if not name:
        return None

    # Map fields to properties
    params = {
        "name": name,
        "dob": clean_str(person_fields.get("dob")),
        "citizenship": clean_str(person_fields.get("citizenship")),
        "place_of_birth": clean_str(person_fields.get("place_of_birth")),
        "phone_number": clean_str(person_fields.get("phone_number")),
        "address": clean_str(person_fields.get("address")),
        "passport_number": clean_str(person_fields.get("passport_number")),
        "flight_number": clean_str(person_fields.get("flight_number")),
        "departure_location": clean_str(person_fields.get("departure_location")),
        "arrival_location": clean_str(person_fields.get("arrival_location")),
        "arrest_location": clean_str(person_fields.get("arrest_location")),
        "arrival_date": clean_str(person_fields.get("arrival_date")),
        "departure_date": clean_str(person_fields.get("departure_date")),
        "date_generic": clean_str(person_fields.get("date_generic")),
        "money": clean_str(person_fields.get("money")),
        "license_plate": clean_str(person_fields.get("license_plate")),
        "drivers_license": clean_str(person_fields.get("drivers_license")),
    }

    # Ensure Person has :Entity:Person labels and uses entity_type='PERSON'
    cypher = """
    MERGE (p:Entity:Person {entity_type: 'PERSON', canonical_text: $name})
    ON CREATE SET
      p.name             = $name,
      p.dob              = $dob,
      p.citizenship      = $citizenship,
      p.place_of_birth   = $place_of_birth,
      p.phone_number     = $phone_number,
      p.address          = $address,
      p.passport_number  = $passport_number,
      p.flight_number    = $flight_number,
      p.departure_location = $departure_location,
      p.arrival_location   = $arrival_location,
      p.arrest_location    = $arrest_location,
      p.arrival_date     = $arrival_date,
      p.departure_date   = $departure_date,
      p.date_generic     = $date_generic,
      p.money            = $money,
      p.license_plate    = $license_plate,
      p.drivers_license  = $drivers_license,
      p.source           = 'structured_text',
      p.created_at       = datetime(),
      p.updated_at       = datetime()
    ON MATCH SET
      p.dob              = coalesce(p.dob, $dob),
      p.citizenship      = coalesce(p.citizenship, $citizenship),
      p.place_of_birth   = coalesce(p.place_of_birth, $place_of_birth),
      p.phone_number     = coalesce(p.phone_number, $phone_number),
      p.address          = coalesce(p.address, $address),
      p.passport_number  = coalesce(p.passport_number, $passport_number),
      p.flight_number    = coalesce(p.flight_number, $flight_number),
      p.departure_location = coalesce(p.departure_location, $departure_location),
      p.arrival_location   = coalesce(p.arrival_location, $arrival_location),
      p.arrest_location    = coalesce(p.arrest_location, $arrest_location),
      p.arrival_date     = coalesce(p.arrival_date, $arrival_date),
      p.departure_date   = coalesce(p.departure_date, $departure_date),
      p.date_generic     = coalesce(p.date_generic, $date_generic),
      p.money            = coalesce(p.money, $money),
      p.license_plate    = coalesce(p.license_plate, $license_plate),
      p.drivers_license  = coalesce(p.drivers_license, $drivers_license),
      p.updated_at       = datetime()
    """
    write(cypher, params)
    return name


def upsert_organization(org_fields: Dict[str, str]) -> Optional[str]:
    name = clean_str(org_fields.get("name"))
    if not name:
        return None
    address = clean_str(org_fields.get("address"))

    params = {"name": name, "address": address}
    cypher = """
    MERGE (o:Entity:Organization {entity_type: 'ORG', canonical_text: $name})
    ON CREATE SET
      o.name       = $name,
      o.address    = $address,
      o.source     = 'structured_text',
      o.created_at = datetime(),
      o.updated_at = datetime()
    ON MATCH SET
      o.address    = coalesce(o.address, $address),
      o.updated_at = datetime()
    """
    write(cypher, params)
    return name


def upsert_location(name: str):
    if not name:
        return
    params = {"name": name}
    cypher = """
    MERGE (l:Entity:Location {entity_type: 'GPE', canonical_text: $name})
    ON CREATE SET
      l.name       = $name,
      l.source     = 'structured_text',
      l.created_at = datetime(),
      l.updated_at = datetime()
    ON MATCH SET
      l.updated_at = datetime()
    """
    write(cypher, params)


def connect_record_to_person(record_id: str, person_name: str):
    params = {"record_id": record_id, "name": person_name}
    cypher = f"""
    MATCH (r:{RECORD_LABEL} {{record_id: $record_id}})
    MATCH (p:Entity:Person {{entity_type:'PERSON', canonical_text:$name}})
    MERGE (r)-[:DESCRIBES]->(p)
    """
    write(cypher, params)


def connect_person_to_org(person_name: str, org_name: str):
    params = {"pname": person_name, "oname": org_name}
    cypher = """
    MATCH (p:Entity:Person {entity_type:'PERSON', canonical_text:$pname})
    MATCH (o:Entity:Organization {entity_type:'ORG', canonical_text:$oname})
    MERGE (p)-[r:ASSOCIATED_WITH_ORG]->(o)
    ON CREATE SET
      r.source     = 'structured_text',
      r.first_seen = datetime(),
      r.last_seen  = datetime()
    ON MATCH SET
      r.last_seen  = datetime()
    """
    write(cypher, params)


def connect_person_to_locations(person_name: str, person_fields: Dict[str, str]):
    for field_key, rel_type in LOCATION_REL_MAP.items():
        loc_name = clean_str(person_fields.get(field_key))
        if not loc_name:
            continue
        upsert_location(loc_name)
        params = {"pname": person_name, "loc_name": loc_name}
        cypher = f"""
        MATCH (p:Entity:Person {{entity_type:'PERSON', canonical_text:$pname}})
        MATCH (l:Entity:Location {{entity_type:'GPE', canonical_text:$loc_name}})
        MERGE (p)-[r:{rel_type}]->(l)
        ON CREATE SET
          r.source     = 'structured_text',
          r.first_seen = datetime(),
          r.last_seen  = datetime()
        ON MATCH SET
          r.last_seen  = datetime()
        """
        write(cypher, params)


# --- MAIN PIPELINE ---
def main():
    print("=== Ensuring schema ===")
    ensure_schema()

    print("=== Loading Excel ===")
    df = load_descriptions(EXCEL_PATH)

    print("=== Processing rows ===")
    for idx, row in df.iterrows():
        description = clean_str(row["description"])
        if not description:
            continue

        record_id = f"{RECORD_ID_PREFIX}{idx+1}"
        upsert_record(record_id, idx + 1, description)

        fields = extract_fields(description)
        person_fields = fields.get("person", {})
        org_fields = fields.get("organization", {})

        person_name = upsert_person(person_fields) if person_fields else None
        org_name = upsert_organization(org_fields) if org_fields else None

        if person_name:
            connect_record_to_person(record_id, person_name)
            connect_person_to_locations(person_name, person_fields)
            if org_name:
                connect_person_to_org(person_name, org_name)

        # Debug
        print(f"[Record {record_id}]")
        print("  description:", description)
        print("  person_fields:", person_fields)
        print("  org_fields:", org_fields)
        print("")

    print("=== Sample graph ===")
    people = read("""
    MATCH (p:Person)
    OPTIONAL MATCH (p)-[:ASSOCIATED_WITH_ORG]->(o:Organization)
    OPTIONAL MATCH (p)-[:DEPARTED_FROM]->(d:Location)
    OPTIONAL MATCH (p)-[:ARRIVED_AT]->(a:Location)
    RETURN p.name AS name,
           p.dob AS dob,
           o.name AS organization,
           d.name AS departed_from,
           a.name AS arrived_at
    LIMIT 20
    """)
    for r in people:
        print(r)


if __name__ == "__main__":
    try:
        main()
    finally:
        driver.close()
