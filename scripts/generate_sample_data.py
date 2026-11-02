"""Generate the synthetic sample data in ``sample_data/``.

The data are SYNTHETIC. They reproduce the release-level shape reported in the
HIFL paper (64 distinct quality events: 35 in R1 hypercare, 11 in R2C1
hypercare, 18 in R2C2/R5 UAT) so that the demo output can be compared with the
paper, but no record comes from a real program.

Run from the repository root:  python scripts/generate_sample_data.py
"""

from __future__ import annotations

import csv
import os
import random
from datetime import datetime, timedelta

random.seed(20261102)
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "sample_data")

# (category hint, business object, title, description, root cause "whys", resolution)
TEMPLATES = [
    ("MAPPING", "REASON_CODE", "Item reason code mismatch after load",
     "Reason codes on migrated sales items do not match the legacy values",
     "Reason code values differ -> conversion rule list incomplete -> mapping did not cover all legacy values",
     "Added missing values to the reason code mapping table"),
    ("MAPPING", "UNIT_OF_MEASURE", "Unit of measure translated incorrectly",
     "UoM on purchase order lines converted to the wrong target unit",
     "Wrong UoM on PO lines -> value translation table outdated -> mapping not reviewed after legacy change",
     "Corrected value translation entries"),
    ("MAPPING", "GL_ACCOUNT", "GL account cross reference missing",
     "Postings fail because legacy GL accounts have no target account",
     "Postings rejected -> xref missing entries -> mapping coverage not reconciled",
     "Completed GL cross reference"),
    ("MASTER_DATA", "SUPPLIER", "Goods supplier blocked for plant",
     "Goods receipts fail because supplier is not extended to the receiving plant",
     "Goods receipt blocked -> supplier location missing -> supplier location governance gap",
     "Extended supplier to plant and added governance check"),
    ("MASTER_DATA", "MATERIAL", "Duplicate material master records",
     "Same material created twice with different numbers",
     "Duplicate material -> no duplicate record check in load -> material master ownership unclear",
     "Merged duplicates and assigned data steward"),
    ("MASTER_DATA", "CUSTOMER", "Customer master missing sales area",
     "Orders cannot be created for migrated customer master records",
     "Order creation fails -> sales area not populated -> business partner template incomplete",
     "Populated sales area data"),
    ("COMPLETENESS", "MO_FILE", "GSC MO files missing records",
     "Manufacturing order files arrive with fewer records than the source extract",
     "Record count lower in target -> extraction scenarios not covered -> missing records for closed orders",
     "Extended extraction selection and added record count check"),
    ("COMPLETENESS", "OPEN_ITEMS", "Open items partially loaded",
     "Only part of the open AR items were loaded into S/4HANA",
     "Balances differ -> partial load of open items -> load job truncated after error",
     "Reloaded remaining open items"),
    ("COMPLETENESS", "INVENTORY", "Inventory row count difference",
     "Row count of stock balances differs between legacy and target",
     "Stock totals differ -> rows dropped in transformation -> filter excluded consignment stock",
     "Fixed transformation filter"),
    ("BUSINESS_LOGIC", "PAYMENT", "Proof pay flag not set",
     "Payments that require proof of payment are released without the flag",
     "Payment released without proof -> flag derivation missing -> business rule not fully implemented",
     "Implemented business rule and added regression test"),
    ("BUSINESS_LOGIC", "PRICING", "Pricing condition calculation wrong",
     "Discount condition calculated on the wrong base value",
     "Wrong net price -> calculation uses gross base -> pricing logic misread in design",
     "Corrected pricing calculation"),
    ("BUSINESS_LOGIC", "TAX", "Tax code derivation incorrect",
     "Tax code derived incorrectly for cross-border deliveries",
     "Wrong tax amount -> tax derivation condition incomplete -> business rule gap",
     "Updated tax derivation logic"),
    ("INTEGRATION", "INVOICE_IDOC", "Invoice IDoc interface failure",
     "Outbound invoice IDocs stuck in error status",
     "Invoices not received -> IDoc failed in middleware -> interface mapping version mismatch",
     "Redeployed interface and added monitoring"),
    ("INTEGRATION", "BANK_API", "Bank statement API timeout",
     "Daily bank statement import times out",
     "Statements missing -> API timeout -> batch job failed under volume",
     "Increased timeout and added alerting"),
    ("SOURCE_SYSTEM", "MATERIAL_LEGACY", "Legacy material data incomplete",
     "Legacy source data has materials without base unit",
     "Load rejects materials -> legacy records missing base unit -> source data entry not validated",
     "Cleansed legacy records"),
    ("SOURCE_SYSTEM", "VENDOR_LEGACY", "Invalid source vendor tax numbers",
     "Upstream legacy system holds invalid vendor tax numbers",
     "Validation errors -> invalid source tax numbers -> historical data never cleansed",
     "Cleansing runbook executed"),
]

RELEASES = [("R1 Hypercare", 35, datetime(2025, 3, 3)), ("R2C1 Hypercare", 11, datetime(2025, 9, 1)), ("R2C2/R5 UAT", 18, datetime(2026, 2, 2))]
PRIORITIES = ["1 - Critical", "2 - High", "3 - Moderate", "4 - Low"]


def build_events():
    events = []
    seq = 0
    for release, n, start in RELEASES:
        for i in range(n):
            # paper's four representative incidents are always present in R1
            if release.startswith("R1") and i < 4:
                t = [TEMPLATES[0], TEMPLATES[3], TEMPLATES[6], TEMPLATES[9]][i]
            else:
                t = random.choice(TEMPLATES)
            seq += 1
            opened = start + timedelta(days=random.randint(0, 40), hours=random.randint(7, 18), minutes=random.choice([0, 15, 30, 45]))
            closed = random.random() < 0.85
            resolved = opened + timedelta(hours=random.randint(2, 120)) if closed else None
            events.append(
                {
                    "seq": seq,
                    "release": release,
                    "cat": t[0],
                    "obj": t[1],
                    "title": f"{t[2]} ({seq:03d})",
                    "description": t[3],
                    "root_cause": t[4],
                    "resolution": t[5] if closed else "",
                    "priority": random.choice(PRIORITIES),
                    "opened": opened,
                    "resolved": resolved,
                    "status": "Resolved" if closed else random.choice(["In Progress", "New"]),
                }
            )
    return events


def write_csv(name, header, rows):
    with open(os.path.join(OUT, name), "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)


def main():
    os.makedirs(OUT, exist_ok=True)
    events = build_events()
    random.shuffle(events)
    sn_events, jira_events = events[:50], events[50:]

    sn_rows = []
    for e in sn_events:
        # one incident is classified manually by the support team (override demo)
        manual = "MASTER_DATA" if e["obj"] == "CUSTOMER" else ""
        sn_rows.append(
            [
                f"INC{100000 + e['seq']}", e["title"], e["description"], e["release"], e["priority"], e["status"],
                e["opened"].strftime("%Y-%m-%d %H:%M:%S"),
                e["resolved"].strftime("%Y-%m-%d %H:%M:%S") if e["resolved"] else "",
                e["obj"], "SAP Data Migration", e["root_cause"], e["resolution"], manual,
            ]
        )
    jira_rows = []
    for e in jira_events:
        jira_rows.append(
            [
                f"HC-{e['seq']}", e["title"], e["description"], e["release"], e["priority"].split(" - ")[-1],
                "Done" if e["resolved"] else "In Progress",
                e["opened"].strftime("%Y-%m-%dT%H:%M:%S"),
                e["resolved"].strftime("%Y-%m-%dT%H:%M:%S") if e["resolved"] else "",
                e["obj"], e["root_cause"], e["resolution"],
            ]
        )
    # three issues raised in BOTH channels (cross-channel duplicates)
    for e in sn_events[:3]:
        jira_rows.append(
            [
                f"HC-D{e['seq']}", e["title"], e["description"], e["release"], "High", "Done",
                (e["opened"] + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S"),
                (e["opened"] + timedelta(hours=30)).strftime("%Y-%m-%dT%H:%M:%S"),
                e["obj"], e["root_cause"], e["resolution"],
            ]
        )

    write_csv(
        "servicenow_incidents.csv",
        ["number", "short_description", "description", "u_release", "priority", "state", "opened_at", "resolved_at",
         "u_business_object", "assignment_group", "u_root_cause", "close_notes", "u_hifl_category"],
        sn_rows,
    )
    write_csv(
        "jira_issues.csv",
        ["key", "summary", "description", "fix_version", "priority", "status", "created", "resolution_date",
         "component", "root_cause", "resolution"],
        jira_rows,
    )

    # ---------------- demo target datasets checked by the generated controls --------------
    codes = ["RC01", "RC02", "RC03", "RC04", "RC05", "RC06"]
    write_csv("demo_legacy_orders.csv", ["order_id", "reason_code", "amount"],
              [[f"SO{i:05d}", random.choice(codes), f"{random.uniform(50, 5000):.2f}"] for i in range(1, 201)])
    # RC06 deliberately has no mapping -> mapping_coverage control fails (blocking, HIGH)
    write_csv("demo_reason_code_map.csv", ["legacy_code", "s4_code"], [[c, "Z" + c[2:]] for c in codes[:-1]])

    suppliers = [f"V{i:04d}" for i in range(1, 31)]
    write_csv("demo_s4_suppliers.csv", ["supplier_id", "name", "country"],
              [[s, f"Supplier {s}", random.choice(["US", "CA", "MX", "DE"])] for s in suppliers])
    locs = [[random.choice(suppliers), f"L{i:03d}", random.choice(["1000", "2000", "3000"])] for i in range(1, 41)]
    locs.append(["V9999", "L999", "1000"])  # orphan supplier -> referential integrity fails (blocking, HIGH)
    write_csv("demo_s4_supplier_locations.csv", ["supplier_id", "location_id", "plant"], locs)

    mo = [[f"MO{i:05d}", f"{random.randint(1, 500)}"] for i in range(1, 121)]
    write_csv("demo_source_mo_files.csv", ["mo_id", "quantity"], mo)
    write_csv("demo_target_mo_files.csv", ["mo_id", "quantity"], mo)  # complete -> passes

    pays = [[f"P{i:05d}", f"{random.uniform(10, 9000):.2f}", random.choice(["Y", "N"]), random.choice(["CHECK", "WIRE", "ACH"])] for i in range(1, 151)]
    write_csv("demo_payments.csv", ["payment_id", "amount", "proof_pay_flag", "payment_method"], pays)

    inv = [[f"INV{i:05d}", f"{random.uniform(100, 20000):.2f}"] for i in range(1, 81)]
    write_csv("demo_source_invoices.csv", ["invoice_id", "amount"], inv)
    write_csv("demo_target_invoices.csv", ["invoice_id", "amount"], inv)  # totals match -> passes

    mats = [[f"M{i:05d}", random.choice(["EA", "KG", "L"])] for i in range(1, 61)]
    mats[7][1] = ""  # one legacy material without base unit -> source profiling fails (MEDIUM, non-blocking)
    write_csv("demo_legacy_materials.csv", ["material_id", "base_uom"], mats)
    write_csv("demo_s4_materials.csv", ["material_id", "base_uom"], [m if m[1] else [m[0], "EA"] for m in mats])
    print(f"Sample data written to {OUT}")


if __name__ == "__main__":
    main()
