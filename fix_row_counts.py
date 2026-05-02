import codecs
import re

with codecs.open('bootstrap_from_excel.py', 'r', 'utf-8') as f:
    content = f.read()

# Update load_policy
content = re.sub(
    r"conn\.commit\(\)\s+print\(f\"Inserted \{inserted\} \{policy_subtype\} records\.\"\)\s+log_etl\(conn, f'bootstrap_policy_\{policy_subtype\}', source_name, len\(df\), inserted\)",
    r"conn.commit()\n    real_count = conn.execute(\"SELECT COUNT(*) FROM policy_company_certification WHERE policy_subtype=? AND source_name=?\", (policy_subtype, source_name)).fetchone()[0]\n    print(f\"Inserted {real_count} {policy_subtype} records.\")\n    log_etl(conn, f'bootstrap_policy_{policy_subtype}', source_name, len(df), real_count)",
    content
)

# Update load_manufacturer
content = re.sub(
    r"conn\.commit\(\)\s+print\(f\"Inserted \{inserted\} manufacturer records\.\"\)\s+log_etl\(conn, 'bootstrap_manufacturer', source_name, len\(df\), inserted\)",
    r"conn.commit()\n    real_count = conn.execute(\"SELECT COUNT(*) FROM company_manufacturer_status WHERE evidence_source=?\", (source_name,)).fetchone()[0]\n    print(f\"Inserted {real_count} manufacturer records.\")\n    log_etl(conn, 'bootstrap_manufacturer', source_name, len(df), real_count)",
    content
)

# Update load_sme_competition
content = re.sub(
    r"conn\.commit\(\)\s+print\(f\"Inserted \{inserted\} SME competition records\.\"\)\s+log_etl\(conn, 'bootstrap_sme_competition', source_name, len\(df\), inserted\)",
    r"conn.commit()\n    real_count = conn.execute(\"SELECT COUNT(*) FROM ref_sme_competition_product WHERE source_name=?\", (source_name,)).fetchone()[0]\n    print(f\"Inserted {real_count} SME competition records.\")\n    log_etl(conn, 'bootstrap_sme_competition', source_name, len(df), real_count)",
    content
)

# Update load_innovation
content = re.sub(
    r"conn\.commit\(\)\s+print\(f\"Inserted \{inserted\} innovation product records\.\"\)\s+log_etl\(conn, 'bootstrap_innovation', source_name, len\(df\), inserted\)",
    r"conn.commit()\n    real_count = conn.execute(\"SELECT COUNT(*) FROM certified_product WHERE certification_type='innovation_product' AND source_name=?\", (source_name,)).fetchone()[0]\n    print(f\"Inserted {real_count} innovation product records.\")\n    log_etl(conn, 'bootstrap_innovation', source_name, len(df), real_count)",
    content
)

with codecs.open('bootstrap_from_excel.py', 'w', 'utf-8') as f:
    f.write(content)
print("Updated all loaders to log real table row counts.")
