#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
xml_to_ndjson.py
Convierte un XML gigantesco a NDJSON con barra de progreso real.
"""

import argparse
import json
import re
from collections import OrderedDict
from lxml import etree
from tqdm import tqdm


def extract_children(elem):
    out = OrderedDict()

    for child in elem:
        tag = etree.QName(child.tag).localname
        node = dict(child.attrib)

        grandchildren = {}
        for gc in child:
            gtag = etree.QName(gc.tag).localname
            entry = dict(gc.attrib)
            text = (gc.text or "").strip()
            if text:
                entry["_text"] = text

            if gtag in grandchildren:
                if isinstance(grandchildren[gtag], list):
                    grandchildren[gtag].append(entry)
                else:
                    grandchildren[gtag] = [grandchildren[gtag], entry]
            else:
                grandchildren[gtag] = entry

        if grandchildren:
            node["_children"] = grandchildren

        if tag in out:
            if isinstance(out[tag], list):
                out[tag].append(node)
            else:
                out[tag] = [out[tag], node]
        else:
            out[tag] = node

    return out


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
            children = extract_children(elem)
            obj = {
                "BayName": name,
                "Attributes": dict(elem.attrib),
                "Children": children
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
