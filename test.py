# Standard library imports
import os
import yaml
import sys
import uuid
import re
import json
import traceback
import zipfile
from xml.etree import ElementTree as ET
import logging
import requests
EYQ_INCUBATOR_ENDPOINT=os.getenv("EYQ_INCUBATOR_ENDPOINT")
EYQ_INCUBATOR_KEY=os.getenv("EYQ_INCUBATOR_KEY")
# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()  # Log to console
    ]
)
logger = logging.getLogger(__name__)

def extract_metadata_map(xml_file_path):
    tree = ET.parse(xml_file_path)
    root = tree.getroot()

    metadata = {
        "job": {
            "name": root.attrib.get("label"),
            "version": root.attrib.get("version")
        },
        "components": [],
        "connections": []
    }

    for node in root.findall(".//node"):
        component = {
            "component_name": node.attrib.get("componentName"),
            "unique_name": node.attrib.get("componentName") + "_" + node.attrib.get("componentVersion", "1.0"),
            "parameters": {param.attrib['name']: param.attrib.get('value') for param in node.findall("elementParameter")},
            "metadata": []
        }

        for md in node.findall(".//metadata"):
            schema = {
                "name": md.attrib.get("name"),
                "columns": []
            }
            for col in md.findall(".//column"):
                schema["columns"].append({
                    "name": col.attrib.get("name"),
                    "type": col.attrib.get("type")
                })
            component["metadata"].append(schema)

        metadata["components"].append(component)

    for conn in root.findall(".//connection"):
        metadata["connections"].append({
            "source": conn.attrib.get("source"),
            "target": conn.attrib.get("target"),
            "label": conn.attrib.get("label"),
            "type": conn.attrib.get("connectorName")
        })
    save_metadata_yml(metadata)
    return metadata

def save_metadata_yml(metadata, output_path="talend_metadata.json"):
    with open(output_path, "w") as f:
        json.dump(metadata, f, sort_keys=False)
    print(f"✅ Metadata saved to {output_path}:{json.dumps(metadata.get('connections', []), indent=2)}")

meta = extract_metadata_map("subjob_CostCentre_Language_SOAR_2.9.item")
#print(yaml.dump(meta, sort_keys=False))
#save_metadata_yml(meta)
def call_generative_model(prompt):
    """
    Call the GPT-4 Turbo model via EYQ Incubator endpoint.
    
    Args:
        prompt (str): The prompt to send to the model.
    
    Returns:
        str: Generated text or error message.
    """
    logger.info(f"Calling GPT-4 Turbo with prompt length: {len(prompt)} characters...")
    headers = {"api-key": EYQ_INCUBATOR_KEY, "Content-Type": "application/json"}
    payload = {
        "model": "gpt-4-turbo",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 2000,
        "temperature": 0.7
    }
    try:
        response = requests.post(
            f"{EYQ_INCUBATOR_ENDPOINT}",
            headers=headers, json=payload, params={"api-version": "2023-05-15"}, timeout=120
        )
        response.raise_for_status()
        result = response.json()
        if not result.get("choices"):
            return f"-- ERROR: Content generation failed (prompt blocked?)."
        return result["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logger.error(f"Error calling GPT-4 Turbo: {e}")
        return f"-- ERROR: Failed to generate content: {e}"