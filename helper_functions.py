import os
from typing import Any
import git
from prefect import task
from pydantic import HttpUrl
import rdflib
import requests
from rdflib.plugins.stores.memory import Memory
import yaml
from pathlib import Path
from datetime import datetime

# res = requests.get(
#     "https://github.com/InTaVia/source-data/raw/feat/update-apis-data-05-05-2023/datasets/apis_data.ttl"
# )
# store = Memory()
# g1 = rdflib.Graph(store, identifier="http://example.org/g1").parse(
#     data=res.text, format="ttl"
# )
# print("test")

# g = rdflib.ConjunctiveGraph()
# res = g.query(
#     """SELECT ?s ?p ?o
#         FROM <https://github.com/InTaVia/source-data/raw/feat/update-apis-data-05-05-2023/datasets/apis_data.ttl>

#         WHERE { ?s ?p ?o } LIMIT 10
#         """
# )
# print(res)

# res2 = g.query(
#     """SELECT ?s ?p ?o
#         FROM <https://github.com/InTaVia/source-data/raw/feat/update-apis-data-05-05-2023/datasets/apis_data.ttl>

#         WHERE { ?s ?p ?o } LIMIT 10 OFFSET 10
#         """
# )
# print(res2)


def serialize_graph(g, storage_path, file_name, add_date_to_file):
    """serializes the RDFLib graph to a given destination

    Args:
        g (rflib): the graph to serialize
        storage_path (str): path within the container to store the file to
        named_graph (uri): optional named graph to serialize

    Returns:
        path: path of the stored file
    """

    Path(storage_path).mkdir(parents=True, exist_ok=True)
    f_path = f"{storage_path}/{file_name}"
    if add_date_to_file:
        f_path += f"_{datetime.now().strftime('%d-%m-%Y')}"
    f_path += ".ttl"
    g.serialize(
        destination=f_path,
        format="turtle",
    )
    return f_path


@task
def create_conjunctive_graph_from_github_branch(
    gh_repo: HttpUrl,
    branch_name: str,
    datasets: list[str] | None = None,
    config: str = "datasets.yml",
):
    g = rdflib.ConjunctiveGraph()
    full_local_path = os.path.join(os.getcwd(), "source-data2")
    # repo = git.Repo.clone_from(gh_repo, full_local_path, branch=branch_name)
    with open(os.path.join(full_local_path, config), "r") as conf:
        conf = yaml.safe_load(conf)
        if datasets is None:
            datasets_paths = [
                (os.path.join(full_local_path, "datasets", d["file"]), d["namespace"])
                for d in conf["datasets"]
            ]
        else:
            datasets_paths = [
                (os.path.join(full_local_path, "datasets", d["file"]), d["namespace"])
                for d in conf["datasets"]
                if d["name"] in datasets
            ]
        for dataset in datasets_paths:
            if not os.path.exists(dataset[0]):
                print(f"File {dataset[0]} does not exist")
                continue
            g.get_context(dataset[1]).parse(dataset[0], format="ttl")
    return g


# create_conjunctive_graph_from_github_branch(
#     "https://github.com/InTaVia/source-data.git", "feat/update-apis-data-05-05-2023"
# )
# print("test")
