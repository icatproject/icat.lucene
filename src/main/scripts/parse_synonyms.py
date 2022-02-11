#!/usr/bin/env python

import csv
import sys
from typing import Dict, List


def addToParents(
    relationships: Dict[str, Dict[str, List[str]]],
    label: str,
    parents: List[str],
    childDepth: int
):
    """
    Adds the `label` to all the entries in `relationships` that have a key in
    `parents`, then recursively calls itself to add `label` to any
    grandparents. `childDepth` is decreased by 1 for each generation to prevent
    exponentially large injections.

    Parameters
    ----------
    relationships: Dict[str, Dict[str, List[str]]]
        Maps terms to an inner dictionary containing arrays for "alternatives",
        "parents", and "children".
    label: str
        The term to be added to its `parents`.
    parents: List[str]
        The direct parents of the current `label`
    childDepth: int
        The number of generations of children to inject for each term.
        For example, a value of 2 would inject children and their children.
        0 will only add alternative terms. Negative integers will add all
        children, grandchildren, etc. Note that this may result in an
        exponentially large number of terms
    """
    if childDepth != 0:
        for parent in parents:
            try:
                relationships[parent]["children"].append(label)
                addToParents(
                    relationships,
                    label,
                    relationships[parent]["parents"],
                    childDepth - 1,
                )
            except KeyError:
                pass


def main(inputFile: str, outputFile: str, mode: str, maxChildDepth: int):
    """
    Reads an CSV file of terminology and writes it into Solr synonym format
    for use in synonym injection. Alternative terms are always written, and the
    number of child terms is configurable by `maxChildDepth`.

    Parameters
    ----------
    inputFile: str
        CSV file to read ontology from.
    outputFile: str
        Solr synonym output file.
    mode: str
        Python file mode (w, a, ...) to use when writing the output file.
    maxChildDepth: int
        The maximum number of generations of children to inject for each term.
        For example, a value of 2 would inject children and their children.
        0 will only add alternative terms. Negative integers will add all
        children, grandchildren, etc. Note that this may result in an
        exponentially large number of terms
    """
    altIndices = []
    parentIndices = []
    # equivalentIndices = []
    relationships = {}
    with open(inputFile) as f:
        reader = csv.reader(f)

        # Dynamically determine header positions
        headers = next(reader)
        for i, header in enumerate(headers):
            if "Label" == header.strip():
                labelIndex = i
            # elif "Class Type" == header:
            #     classIndex = i
            elif "Alt Label" in header.strip():
                altIndices.append(i)
            elif "Parent IRI" == header.strip():
                parentIndices.append(i)
            # elif "Equivalent" == header.strip():
            #     equivalentIndices.append(i)

        for entries in reader:
            try:
                int(entries[0])
            except (ValueError, IndexError):
                # If we do not have an ID, continue to the next line
                continue

            label = entries[labelIndex]
            if label in relationships.keys():
                raise ValueError(f"Duplicate entry for label {label}")

            # relationships[label] = {
            #     "alternatives": [],
            #     "parents": [],
            #     "equivalent": [],
            #     "children": [],
            # }
            relationships[label] = {
                "alternatives": [], "parents": [], "children": []
            }
            # classType = entries[classIndex]
            for altIndex in altIndices:
                alternativeLabel = entries[altIndex]
                if alternativeLabel != "":
                    relationships[label]["alternatives"].append(
                        alternativeLabel
                    )
            for parentIndex in parentIndices:
                parent = entries[parentIndex]
                if parent != "":
                    relationships[label]["parents"].append(parent)
            # for equivalentIndex in equivalentIndices:
            #     equivalentLabel = entries[equivalentIndex]
            #     if equivalentLabel != "":
            #         relationships[label]["equivalent"].append(equivalentLabel)

    print(f"{len(relationships)} relationships found")
    for label, relationship in relationships.items():
        addToParents(
            relationships, label, relationship["parents"], maxChildDepth
        )

    output = ""
    for label, relationship in relationships.items():
        # Only write to file if we have alternative or child terms
        if (len(relationship["alternatives"]) > 0
                or len(relationship["children"]) > 0):
            leftHandSide = ", ".join(
                set([label] + relationship["alternatives"])
            )
            rightHandSide = ", ".join(
                set(
                    [label]
                    + relationship["alternatives"]
                    + relationship["children"]
                )
            )
            output += leftHandSide + " => " + rightHandSide + "\n"

    with open(outputFile, mode) as f:
        f.write(output)


if __name__ == "__main__":
    args = sys.argv
    try:
        inputFile = args[1]
    except IndexError as e:
        raise IndexError("inputFile to parse not provided") from e
    try:
        outputFile = args[2]
    except IndexError as e:
        raise IndexError("outputFile to write to not provided") from e
    try:
        mode = args[3]
    except IndexError:
        # Default to appending to the outputFile (no overwrite)
        mode = "a"
    try:
        maxChildDepth = int(args[4])
    except (IndexError, ValueError):
        # Default to 0 depth (only alternative terms)
        maxChildDepth = 0

    main(inputFile, outputFile, mode, maxChildDepth)
