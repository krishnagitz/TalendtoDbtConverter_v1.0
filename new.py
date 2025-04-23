#!/usr/bin/env python3
import os
import sys
import uuid
import re
import json
import traceback
from dotenv import load_dotenv

load_dotenv()  # Load .env file

#api_key = os.getenv("GEN_API_KEY")

from flask import Flask, request, jsonify, send_from_directory
from werkzeug.utils import secure_filename
from flask_cors import CORS
import google.generativeai as genai

# ==== CONFIGURATION ====
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR  = os.path.join(BASE_DIR, "uploads")
RESULTS_DIR = os.path.join(BASE_DIR, "results")
for d in (UPLOAD_DIR, RESULTS_DIR):
    os.makedirs(d, exist_ok=True)
print(os.getenv("GEN_API_KEY"))
API_KEY = os.getenv("GEN_API_KEY")
if not API_KEY:
    print("Please set GEN_API_KEY in your environment", file=sys.stderr)
    sys.exit(1)

genai.configure(api_key=API_KEY)
MODEL_NAME = "gemini-2.0-flash"

# ==== FLASK SETUP ====
app = Flask(__name__, static_folder=BASE_DIR, static_url_path="")
CORS(app)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16 MB

# Serve the HTML UI
@app.route("/", methods=["GET"])
def index():
    return send_from_directory(BASE_DIR, "talendtodbt.html")

# Convert endpoint: XML → raw_output + SQL skeleton
@app.route("/convert", methods=["POST"])
def convert():
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file part in the request"}), 400
        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No file selected"}), 400

        job_id   = str(uuid.uuid4())
        xml_name = secure_filename(f"{job_id}.item")
        xml_path = os.path.join(UPLOAD_DIR, xml_name)
        file.save(xml_path)

        with open(xml_path, "r", encoding="utf-8", errors="replace") as f:
            xml_content = f.read()

        prompt = (
            "You are a Data Build Tool expert.\n"
            "Convert the following Talend item XML into corresponding dbt models:\n\n"
            + xml_content
        )

        model    = genai.GenerativeModel(model_name=MODEL_NAME)
        response = model.generate_content(prompt)
        raw_txt  = response.text

        # Save raw .txt
        txt_filename = f"{job_id}.txt"
        txt_path     = os.path.join(RESULTS_DIR, txt_filename)
        with open(txt_path, "w", encoding="utf-8") as out:
            out.write(raw_txt)

        # Extract SQL code blocks or use full text
        blocks = re.findall(r"```(?:sql)?\n(.*?)\n```", raw_txt, flags=re.S)
        if not blocks:
            blocks = [raw_txt]

        # Save initial .sql
        sql_filename = f"{job_id}.sql"
        sql_path     = os.path.join(RESULTS_DIR, sql_filename)
        with open(sql_path, "w", encoding="utf-8") as out:
            for i, blk in enumerate(blocks, start=1):
                out.write(f"-- dbt model #{i}\n")
                out.write(blk.strip() + "\n\n")

        host_url = request.host_url.rstrip("/")
        return jsonify({
            "raw_output": raw_txt,
            "files": {
                "raw_txt": f"{host_url}/download/{txt_filename}",
                "sql":     f"{host_url}/download/{sql_filename}"
            }
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# Generate commented SQL from convert() JSON
@app.route("/generate_sql", methods=["POST"])
def generate_sql():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON payload provided"}), 400

        prompt = (
            "The following JSON represents dbt model SQL skeletons extracted from a Talend job.\n"
            "Generate a single, properly formatted SQL file with detailed comments for each model make sure not not exclude anything in the json:\n"
            + json.dumps(data, indent=2)
        )

        model    = genai.GenerativeModel(model_name=MODEL_NAME)
        response = model.generate_content(prompt)
        sql_text = response.text

        # Save the commented SQL
        job_id        = str(uuid.uuid4())
        commented_fn  = secure_filename(f"{job_id}_commented.sql")
        commented_path= os.path.join(RESULTS_DIR, commented_fn)
        with open(commented_path, "w", encoding="utf-8") as out:
            out.write(sql_text)

        host_url = request.host_url.rstrip("/")
        return jsonify({
            "sql_output": sql_text,
            "file_url":   f"{host_url}/download/{commented_fn}"
        }), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# Download any generated file
@app.route("/download/<filename>", methods=["GET"])
def download(filename):
    return send_from_directory(RESULTS_DIR, filename, as_attachment=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
