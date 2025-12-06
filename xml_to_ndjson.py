#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
xml_to_ndjson.py
Convierte un XML gigantesco a NDJSON con barra de progreso real.

Uso:
    python xml_to_ndjson.py --xml IMM_EPM.xml --out bays.jsonl
"""

import argparse
import json
import re
from collections import OrderedDict
from lxml import etree
from tqdm import tqdm


def xml_to_obj(elem):
    obj = {}

    # 1. Agregar atributos
    if elem.attrib:
        obj.update(dict(elem.attrib))

    # 2. Texto interno
    text = (elem.text or "").strip()
    if text:
        obj["_text"] = text

    # 3. Hijos
    children = {}
    for child in elem:
        tag = etree.QName(child.tag).localname
        child_obj = xml_to_obj(child)

        if tag in children:
            # Convertir en lista (si aún no lo es)
            if not isinstance(children[tag], list):
                children[tag] = [children[tag]]
            children[tag].append(child_obj)
        else:
            children[tag] = child_obj

    if children:
        obj["_children"] = children

    return obj


def xml_to_ndjson(xml_path, out_path, regex=r"^R\d{1,4}$"):
    pattern = re.compile(regex)

    # Calculamos tamaño total del archivo
    import os
    file_size = os.path.getsize(xml_path)

    # Abrimos archivo manualmente para monitorear lectura
    f = open(xml_path, "rb")

    # Creamos parser manual basado en file object
    context = etree.iterparse(
        f,
        events=("end",),
        tag="Bay",
        recover=True,
        huge_tree=True
    )

    out = open(out_path, "w", encoding="utf-8")

    count = 0
    last_pos = 0

    # Barra de progreso basada en bytes leídos
    pbar = tqdm(total=file_size, unit="B", unit_scale=True, desc="Procesando XML")

    for event, elem in context:
        # Actualizar progreso según bytes leídos reales
        pos = f.tell()
        pbar.update(pos - last_pos)
        last_pos = pos

        name = elem.get("Name", "")
        if name and pattern.fullmatch(name):
            children = xml_to_obj(elem)
            obj = {
                "BayName": name,
                "Attributes": dict(elem.attrib),
                "Children": children.get("_children", {})
            }
            out.write(json.dumps(obj, ensure_ascii=False) + "\n")
            count += 1

        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

    f.close()
    out.close()
    pbar.close()

    return count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--xml", required=True)
    parser.add_argument("--out", default="bays.jsonl")
    parser.add_argument("--regex", default=r"^R\d{1,4}$")
    args = parser.parse_args()

    n = xml_to_ndjson(args.xml, args.out, args.regex)
    print(f"\nListo. Bays exportadas: {n}")
    print(f"Archivo NDJSON: {args.out}")


if __name__ == "__main__":
    main()
