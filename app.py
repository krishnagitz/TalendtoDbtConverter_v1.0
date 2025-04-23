#!/usr/bin/env python3
import os
import sys
import re
import google.generativeai as genai

# ==== CONFIGURATION ====
API_KEY = os.getenv("GEN_API_KEY")
if not API_KEY:
    print("❌ Please set GEN_API_KEY in your environment.", file=sys.stderr)
    sys.exit(1)

# Initialize the SDK
genai.configure(api_key=API_KEY)

# Model choice
MODEL_NAME = "gemini-2.0-flash"  # or "chat-bison-001"

# File paths
INPUT_XML   = "/Users/vigneshvars/Documents/EYWork/KrishnaBroWork/demo.item"
RAW_OUTPUT  = "dbt_conversion_output.txt"
SQL_OUTPUT  = "dbt_models.sql"

# ==== STEP 1: Read the Talend XML file ====
try:
    with open(INPUT_XML, "r", encoding="utf-8") as f:
        xml_content = f.read()
except Exception as e:
    print(f"❌ Error reading `{INPUT_XML}`: {e}", file=sys.stderr)
    sys.exit(1)

# ==== STEP 2: Build and send the prompt ====
prompt = (
    "You are a Data Build Tool expert.\n"
    "Convert the following Talend item XML into its corresponding dbt models.\n\n"
    + xml_content
)

try:
    model = genai.GenerativeModel(model_name=MODEL_NAME)
    response = model.generate_content(prompt)
    output = response.text
except Exception as e:
    print(f"❌ API call failed: {e}", file=sys.stderr)
    sys.exit(1)

# ==== STEP 3: Save the raw output to text file ====
with open(RAW_OUTPUT, "w", encoding="utf-8") as f:
    f.write(output)
print(f"✅ Raw output saved to `{RAW_OUTPUT}`")

# ==== STEP 4: Extract code blocks and write SQL file ====
# Look for fenced code blocks: ``` or ```sql
code_blocks = re.findall(r"```(?:sql)?\n(.*?)\n```", output, flags=re.S)

if not code_blocks:
    # Fallback: assume entire output is SQL
    code_blocks = [output]

with open(SQL_OUTPUT, "w", encoding="utf-8") as sql_f:
    for idx, block in enumerate(code_blocks, start=1):
        header = f"-- dbt model #{idx}\n"
        sql_f.write(header)
        # Strip leading/trailing blank lines
        sql_f.write(block.strip() + "\n\n")

print(f"✅ Detected {len(code_blocks)} code block(s) and wrote to `{SQL_OUTPUT}`")