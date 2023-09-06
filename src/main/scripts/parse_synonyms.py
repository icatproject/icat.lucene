#!/usr/bin/env python3

import csv
import sys
from typing import Dict, List


def add_to_parents(
    relationships: Dict[str, Dict[str, List[str]]],
    label: str,
    parents: List[str],
    child_depth: int
):
    """
    Adds the `label` to all the entries in `relationships` that have a key in
    `parents`, then recursively calls itself to add `label` to any
    grandparents. `child_depth` is decreased by 1 for each generation to
    prevent exponentially large injections.

    Parameters
    ----------
    relationships: Dict[str, Dict[str, List[str]]]
        Maps terms to an inner dictionary containing arrays for "alternatives",
        "parents", and "children".
    label: str
        The term to be added to its `parents`.
    parents: List[str]
        The direct parents of the current `label`
    child_depth: int
        The number of generations of children to inject for each term.
        For example, a value of 2 would inject children and their children.
        0 will only add alternative terms. Negative integers will add all
        children, grandchildren, etc. Note that this may result in an
        exponentially large number of terms
    """
    if child_depth != 0:
        for parent in parents:
            try:
                relationships[parent]["children"].append(label)
                # If the parent is equivalent to anything, also add label as a
                # child of the equivalent_parent
                for equivalent_parent in relationships[parent]["equivalent"]:
                    relationships[equivalent_parent]["children"].append(label)
                add_to_parents(
                    relationships,
                    label,
                    relationships[parent]["parents"],
                    child_depth - 1,
                )
            except KeyError:
                pass


def main(input_file: str, output_file: str, mode: str, max_child_depth: int):
    """
    Reads an CSV file of terminology and writes it into Solr synonym format
    for use in synonym injection. Alternative terms are always written, and the
    number of child terms is configurable by `max_child_depth`.

    Parameters
    ----------
    input_file: str
        CSV file to read ontology from.
    output_file: str
        Solr synonym output file.
    mode: str
        Python file mode (w, a, ...) to use when writing the output file.
    max_child_depth: int
        The maximum number of generations of children to inject for each term.
        For example, a value of 2 would inject children and their children.
        0 will only add alternative terms. Negative integers will add all
        children, grandchildren, etc. Note that this may result in an
        exponentially large number of terms
    """
    alt_indices = []
    parent_indices = []
    equivalent_indices = []
    equivalent_pairs = {}
    relationships = {}
    with open(input_file) as f:
        reader = csv.reader(f)

        # Dynamically determine header positions
        headers = next(reader)
        for i, header in enumerate(headers):
            if "Label" == header.strip():
                label_index = i
            elif "Alt Label" in header.strip():
                alt_indices.append(i)
            elif "Parent IRI" == header.strip():
                parent_indices.append(i)
            elif "Equivalent" == header.strip():
                equivalent_indices.append(i)

        for entries in reader:
            try:
                int(entries[0])
            except (ValueError, IndexError):
                # If we do not have an ID, continue to the next line
                continue

            label = entries[label_index]
            if label in relationships.keys():
                raise ValueError(f"Duplicate entry for label {label}")

            relationships[label] = {
                "alternatives": [],
                "parents": [],
                "equivalent": [],
                "children": [],
            }
            for alt_index in alt_indices:
                alternative_label = entries[alt_index]
                if alternative_label:
                    relationships[label]["alternatives"].append(
                        alternative_label
                    )
            for parent_index in parent_indices:
                parent = entries[parent_index]
                if parent:
                    relationships[label]["parents"].append(parent)
            for equivalent_index in equivalent_indices:
                equivalent_label = entries[equivalent_index]
                if equivalent_label:
                    relationships[label]["equivalent"].append(equivalent_label)
                    equivalent_pairs[equivalent_label] = label

    # If A is equivalent to B, then also set B equivalent to A
    # This ensures they share all children
    for key, value in equivalent_pairs.items():
        try:
            relationships[key]["equivalent"].append(value)
        except KeyError:
            pass

    print(f"{len(relationships)} relationships found")
    for label, relationship in relationships.items():
        add_to_parents(
            relationships, label, relationship["parents"], max_child_depth
        )

    output = ""
    for label, relationship in relationships.items():
        # Only write to file if we have alternative or child terms
        if (len(relationship["alternatives"]) > 0
                or len(relationship["children"]) > 0):
            left_hand_side = ", ".join(
                sorted(set([label] + relationship["alternatives"]))
            )
            right_hand_side = ", ".join(
                sorted(set(
                    [label]
                    + relationship["alternatives"]
                    + relationship["children"]
                ))
            )
            output += left_hand_side + " => " + right_hand_side + "\n"

    with open(output_file, mode) as f:
        f.write(output)


if __name__ == "__main__":
    args = sys.argv
    try:
        input_file = args[1]
    except IndexError as e:
        raise IndexError("input_file to parse not provided") from e
    try:
        output_file = args[2]
    except IndexError as e:
        raise IndexError("output_file to write to not provided") from e
    try:
        mode = args[3]
    except IndexError:
        # Default to appending to the output_file (no overwrite)
        mode = "a"
    try:
        max_child_depth = int(args[4])
    except (IndexError, ValueError):
        # Default to 0 depth (only alternative terms)
        max_child_depth = 0

    main(input_file, output_file, mode, max_child_depth)
