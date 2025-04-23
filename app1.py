#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Standard library imports
import os
import sys
import uuid
import re
import json
import traceback
import zipfile
from xml.etree import ElementTree as ET

# Third-party imports
from dotenv import load_dotenv
from flask import Flask, request, jsonify, send_from_directory
from werkzeug.utils import secure_filename
from flask_cors import CORS
import google.generativeai as genai

# ==============================================================================
# Configuration & Initialization
# ==============================================================================

# Load environment variables
load_dotenv()
API_KEY = os.getenv("GEN_API_KEY")

# Validate API Key
if not API_KEY:
    print("Error: GEN_API_KEY not found in environment variables or .env file.", file=sys.stderr)
    sys.exit(1)

# Configure Google Generative AI client
try:
    genai.configure(api_key=API_KEY)
    print("Google Generative AI client configured successfully.")
except Exception as config_err:
    print(f"Error configuring Google Generative AI: {config_err}", file=sys.stderr)
    sys.exit(1)

# Model Selection
MODEL_NAME = "gemini-1.5-flash-latest"
print(f"Using Generative Model: {MODEL_NAME}")

# Directory Setup
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
RESULTS_DIR = os.path.join(BASE_DIR, "results")
for d in (UPLOAD_DIR, RESULTS_DIR):
    os.makedirs(d, exist_ok=True)
print(f"Uploads directory: {UPLOAD_DIR}")
print(f"Results directory: {RESULTS_DIR}")

# Flask Application Setup
app = Flask(__name__, static_folder=BASE_DIR, static_url_path="")
CORS(app)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16 MB
print("Flask app initialized with CORS enabled.")

# ==============================================================================
# Helper Functions
# ==============================================================================

def parse_talend_xml(xml_content):
    """
    Parse Talend XML (.item file) to extract components, connections, metadata, and notes.
    
    Args:
        xml_content (str): Content of the Talend XML file.
    
    Returns:
        dict: Summary of job structure or error details.
    """
    summary = {
        "job_name": "unknown_job",
        "components": [],
        "connections": [],
        "metadata": {},
        "notes": []
    }
    print("Starting XML parsing...")
    try:
        namespaces = {
            'TalendProperties': 'http://www.talend.org/properties',
            'xmi': 'http://www.omg.org/XMI',
            'talendfile': 'platform:/resource/org.talend.model/model/TalendFile.xsd',
        }
        root = ET.fromstring(xml_content)
        print("XML root element parsed.")

        # Extract Job Name
        job_info = root.find('.//TalendProperties:Property', namespaces=namespaces)
        if job_info is not None and job_info.get("label"):
            summary["job_name"] = job_info.get("label")
        else:
            process_type = root.find('.//processType')
            if process_type is not None and process_type.get("name"):
                summary["job_name"] = process_type.get("name")
        print(f"Found job name: {summary['job_name']}")

        # Extract Components
        for node in root.findall('.//node'):
            component_name = node.get('componentName')
            unique_name = node.get('uniqueName')
            label = unique_name
            params = {}
            for param in node.findall('.//elementParameter'):
                field = param.get('field')
                name = param.get('name')
                value = param.text if param.text is not None else param.get('value')
                if value is not None and field and name and name not in [
                    "UNIQUE_NAME", "COMPONENT_NAME", "LABEL", "CONNECTION_FORMAT",
                    "CHECK_NUM", "CHECK_UNIQUE_NAME", "ACTIVATE", "LOG4J_ACTIVATE",
                    "START", "STARTABLE", "SUBTREE_START", "END_OF_FLOW", "ACTIVATE",
                    "PROCESS_TYPE_VERSION", "PROCESS_TYPE_CONTEXT", "PROCESS_TYPE_PROCESS",
                    "QUERYSTORE", "UPDATE_COMPONENTS", "CURRENT_OS"
                ]:
                    cleaned_value = value.strip('"') if isinstance(value, str) else value
                    params[name] = cleaned_value
                if component_name == 'tMap':
                    if name == 'VAR_TABLE': params['tMap_variables_raw'] = value
                    if name == 'OUTPUT_TABLES': params['tMap_outputs_raw'] = value
                elif name == 'QUERY' and value:
                    params['sql_query'] = value.strip('"') if isinstance(value, str) else value
                elif name == 'FILENAME' and value:
                    params['filepath'] = value.strip('"') if isinstance(value, str) else value
                elif name == 'TABLE' and value:
                    params['db_table_name'] = value.strip('"') if isinstance(value, str) else value

            label_param = node.find('.//elementParameter[@name="LABEL"]')
            if label_param is not None and label_param.get('value'):
                label = label_param.get('value').strip('"')
            hint_param = node.find('.//elementParameter[@name="HINT"]')
            if hint_param is not None and hint_param.get('value'):
                params["hint"] = hint_param.get('value').strip('"')

            component_data = {
                "type": component_name, "unique_name": unique_name, "label": label,
                "parameters": params, "metadata": []
            }

            for meta_conn in node.findall('.//metadata'):
                connector_name = meta_conn.get('connector')
                meta_name = meta_conn.get('name')
                columns = []
                for col in meta_conn.findall('.//column'):
                    col_data = {
                        "name": col.get('name'), "talend_type": col.get('type'),
                        "key": col.get('key', 'false') == 'true',
                        "nullable": col.get('nullable', 'true') == 'true',
                        "length": col.get('length'), "precision": col.get('precision'),
                        "comment": col.get('comment')
                    }
                    default_val = col.get('defaultValue')
                    if default_val: col_data['default'] = default_val.strip('"')
                    columns.append(col_data)
                component_data["metadata"].append({
                    "connector_type": connector_name, "name": meta_name, "columns": columns
                })
                if meta_name and meta_name not in summary["metadata"]:
                    summary["metadata"][meta_name] = {"columns": columns}
            summary["components"].append(component_data)

        # Extract Connections
        for conn in root.findall('.//connection'):
            summary["connections"].append({
                "source_component_id": conn.get('source'), "target_component_id": conn.get('target'),
                "connector_name": conn.get('connectorName'), "line_style": conn.get('lineStyle'),
                "metadata_name": conn.get('metaname')
            })

        # Extract Notes
        for note in root.findall('.//note'):
            summary["notes"].append(note.get('text'))
        print("XML parsing finished successfully.")

    except ET.ParseError as e:
        print(f"XML Parsing Error: {e}", file=sys.stderr)
        summary["error"] = f"Failed to parse XML: {e}"
    except Exception as e:
        print(f"Unexpected Error during XML parsing: {e}", file=sys.stderr)
        summary["error"] = f"Unexpected error during XML parsing: {e}"
    return summary

def call_generative_model(prompt, model_name=MODEL_NAME):
    """
    Call the generative model with the provided prompt.
    
    Args:
        prompt (str): The prompt to send to the model.
        model_name (str): The model to use.
    
    Returns:
        str: Generated text or error message.
    """
    print(f"Calling Generative Model ({model_name})...")
    try:
        model = genai.GenerativeModel(model_name=model_name)
        response = model.generate_content(prompt)
        if not response.candidates:
            return f"-- ERROR: Content generation failed (prompt blocked?)."
        candidate = response.candidates[0]
        if not candidate.content or not candidate.content.parts:
            return f"-- ERROR: No content generated."
        return candidate.content.parts[0].text
    except Exception as e:
        print(f"Error calling Generative Model: {e}", file=sys.stderr)
        return f"-- ERROR: Failed to generate content: {e}"

def create_dbt_project_files(job_id, safe_filename, dbt_content):
    """
    Create dbt project files and zip them.
    
    Args:
        job_id (str): Unique job identifier.
        safe_filename (str): Sanitized filename.
        dbt_content (str): Generated dbt content.
    
    Returns:
        tuple: (zip_path, zip_filename)
    """
    dbt_dir = os.path.join(RESULTS_DIR, f"dbt_project_{job_id}")
    os.makedirs(dbt_dir, exist_ok=True)
    models_dir = os.path.join(dbt_dir, "models")
    os.makedirs(models_dir, exist_ok=True)

    # Extract SQL blocks
    sql_blocks = re.findall(r"```(?:sql)?\n(.*?)\n```", dbt_content, flags=re.DOTALL)
    if not sql_blocks:
        sql_blocks = [dbt_content]
        
    # Save SQL models
    sql_filename = f"{safe_filename}.sql"
    sql_path = os.path.join(models_dir, sql_filename)
    with open(sql_path, "w", encoding="utf-8") as f:
        for i, block in enumerate(sql_blocks, start=1):
            f.write(f"-- dbt model #{i}\n{block.strip()}\n\n")

    # Create basic dbt_project.yml
    dbt_project_yml = """
name: 'talend_converted_project'
version: '1.0.0'
config-version: 2
profile: 'default'
model-paths: ["models"]
    """
    with open(os.path.join(dbt_dir, "dbt_project.yml"), "w", encoding="utf-8") as f:
        f.write(dbt_project_yml)

    # Zip the dbt project
    zip_filename = f"{job_id}_{safe_filename}_dbt_project.zip"
    zip_path = os.path.join(RESULTS_DIR, zip_filename)
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(dbt_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, dbt_dir)
                zipf.write(file_path, arcname)

    return zip_path, zip_filename

# ==============================================================================
# Flask Routes
# ==============================================================================

@app.route("/", methods=["GET"])
def index():
    """Serve the main HTML page."""
    print("Serving talendtodbt.html")
    try:
        return send_from_directory(BASE_DIR, "talendtodbt.html")
    except FileNotFoundError:
        print("Error: talendtodbt.html not found", file=sys.stderr)
        return jsonify({"error": "UI file not found"}), 404

@app.route("/convert", methods=["POST"])
def convert():
    """Handle file upload, XML parsing, and dbt conversion."""
    print("Received request on /convert endpoint.")
    if 'file' not in request.files:
        return jsonify({"error": "No file part in the request"}), 400
    file = request.files['file']
    if not file or not file.filename:
        return jsonify({"error": "No file selected"}), 400
    if not file.filename.lower().endswith(('.xml', '.item')):
        return jsonify({"error": "Invalid file type. Upload '.item' or '.xml'."}), 400

    # File Handling
    job_id = str(uuid.uuid4())
    safe_filename = secure_filename(os.path.splitext(file.filename)[0]) or "talend_job"
    xml_name = f"{job_id}_{safe_filename}.item"
    xml_path = os.path.join(UPLOAD_DIR, xml_name)
    raw_filename = f"{job_id}_llm_raw_output.txt"
    raw_filepath = os.path.join(RESULTS_DIR, raw_filename)
    sql_filename = f"{job_id}_{safe_filename}.sql"
    sql_path = os.path.join(RESULTS_DIR, sql_filename)

    try:
        file.save(xml_path)
        print(f"Uploaded file saved to: {xml_path}")

        with open(xml_path, "r", encoding="utf-8", errors="replace") as f:
            xml_content = f.read()
        if not xml_content:
            return jsonify({"error": "XML file is empty", "job_id": job_id}), 400

        # Parse XML
        talend_summary = parse_talend_xml(xml_content)
        if "error" in talend_summary:
            return jsonify({"error": talend_summary["error"], "job_id": job_id}), 500

        # Create LLM Prompt
        prompt = f"""
You are an expert Talend to dbt conversion assistant. Convert the provided Talend job XML or its parsed summary into corresponding dbt models (SQL and YAML configuration). Use the parsed summary if available, otherwise analyze the raw XML.

**Parsed Talend Job Summary:**
Job Name: {talend_summary.get('job_name', 'N/A')}

Components:
```json
{json.dumps(talend_summary.get('components', []), indent=2)}
```

Connections:
```json
{json.dumps(talend_summary.get('connections', []), indent=2)}
```

Metadata:
```json
{json.dumps(talend_summary.get('metadata', {}), indent=2)}
```

Notes:
```json
{json.dumps(talend_summary.get('notes', []), indent=2)}
```

**Instructions:**
1. Generate dbt SQL models (.sql files) based on the Talend job logic.
2. Include a `dbt_project.yml` configuration file.
3. Provide detailed comments in the SQL files.
4. Handle tMap components by converting mappings to SQL transformations.
5. Return SQL code blocks marked by ```sql\n...\n```.
"""
        # Call LLM
        dbt_content = call_generative_model(prompt)
        if dbt_content.startswith("-- ERROR"):
            return jsonify({"error": dbt_content, "job_id": job_id}), 500

        # Save Raw Output
        with open(raw_filepath, "w", encoding="utf-8") as f:
            f.write(dbt_content)

        # Extract SQL Blocks
        sql_blocks = re.findall(r"```(?:sql)?\n(.*?)\n```", dbt_content, flags=re.DOTALL)
        if not sql_blocks:
            sql_blocks = [dbt_content]

        # Save SQL File
        with open(sql_path, "w", encoding="utf-8") as f:
            for i, block in enumerate(sql_blocks, start=1):
                f.write(f"-- dbt model #{i}\n{block.strip()}\n\n")

        # Create dbt Project ZIP
        zip_path, zip_filename = create_dbt_project_files(job_id, safe_filename, dbt_content)

        host_url = request.host_url.rstrip("/")
        return jsonify({
            "job_id": job_id,
            "raw_output": dbt_content,
            "files": {
                "raw_txt": f"{host_url}/download/{raw_filename}",
                "sql": f"{host_url}/download/{sql_filename}",
                "dbt_project_zip": f"{host_url}/download/{zip_filename}"
            }
        }), 200

    except Exception as e:
        print(f"Error in /convert: {e}", file=sys.stderr)
        traceback.print_exc()
        return jsonify({"error": str(e), "job_id": job_id}), 500

@app.route("/generate_sql", methods=["POST"])
def generate_sql():
    """Generate commented SQL from JSON input."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON payload provided"}), 400

        prompt = f"""
You are a Data Build Tool expert. Generate a properly formatted SQL file with detailed comments based on the Talend job that was converted to dbt. Analyze the content provided and create comprehensive SQL models:

Raw Output:
{data.get('raw_output', '(No raw output provided)')}

**Instructions:**
1. Create a single SQL file combining all models.
2. Add detailed comments explaining the purpose and logic of each model.
3. Ensure dbt compatibility with proper syntax.
4. Return the SQL content as plain text.
"""
        sql_text = call_generative_model(prompt)
        if sql_text.startswith("-- ERROR"):
            return jsonify({"error": sql_text}), 500

        job_id = data.get('job_id', str(uuid.uuid4()))
        commented_fn = f"{job_id}_commented.sql"
        commented_path = os.path.join(RESULTS_DIR, commented_fn)
        with open(commented_path, "w", encoding="utf-8") as f:
            f.write(sql_text)

        host_url = request.host_url.rstrip("/")
        return jsonify({
            "sql_output": sql_text,
            "file_url": f"{host_url}/download/{commented_fn}"
        }), 200
    except Exception as e:
        print(f"Error in /generate_sql: {e}", file=sys.stderr)
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route("/download/<filename>", methods=["GET"])
def download(filename):
    """Download a generated file."""
    try:
        return send_from_directory(RESULTS_DIR, filename, as_attachment=True)
    except FileNotFoundError:
        print(f"Error: File {filename} not found in {RESULTS_DIR}", file=sys.stderr)
        return jsonify({"error": "File not found"}), 404

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)