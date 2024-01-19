from typing import List
from venv import create
from SPARQLWrapper import SPARQLWrapper, JSON
from pydantic import BaseModel, DirectoryPath, Field, HttpUrl
from requests.auth import HTTPBasicAuth
from rdflib import URIRef, Namespace, Graph, Literal, XSD
from rdflib.namespace import OWL, RDF, RDFS
import prefect
from prefect import Flow, flow, get_run_logger, task
from prefect.context import TaskRunContext, FlowRunContext
import rdflib
import git
import yaml
import requests
import os
from string import Template
import datetime
from helper_functions import serialize_graph
from push_rdf_file_to_github import push_data_to_repo_flow, Params as ParamsPush

IDM_PROV = Namespace("http://www.intavia.eu/idm-prov/")
IDM_PREFECT = Namespace("http://www.intavia.eu/idm-prefect/")
PROV = Namespace("http://www.w3.org/ns/prov#")
PROV_TARGET_GRAPH = "http://www.intavia.org/graphs/provenance"


@task()
def setup_sparql_connection(endpoint):
    sparql = SPARQLWrapper(endpoint)
    sparql.setHTTPAuth("BASIC")
    sparql.setCredentials(
        user=os.environ.get("RDFDB_USER"), passwd=os.environ.get("RDFDB_PASSWORD")
    )
    return sparql


@task()
def create_conjunctive_graph_from_github_branch(
    gh_repo: HttpUrl,
    branch_name: str = "main",
    datasets: list[str] | None = None,
    config: str = "datasets.yml",
):
    logger = get_run_logger()
    g = rdflib.ConjunctiveGraph()
    full_local_path = os.path.join(os.getcwd(), "source-data")
    logger.info(f"Cloning {gh_repo}/{branch_name} to {full_local_path}")
    repo = git.Repo.clone_from(gh_repo, full_local_path, branch=branch_name)
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
                logger.warning(f"File {dataset[0]} does not exist")
                continue
            g.get_context(dataset[1]).parse(dataset[0], format="ttl")
    return g


@task
def get_sameas_statements(
    sparql,
    entity_source_uris,
    entity_linker_graph,
    entity_source_type,
    entity_source_proxy_type,
):
    logger = get_run_logger()

    # from_part = "\n".join(
    #     list(
    #         map(
    #             lambda x: "from <" + x + ">", entity_source_uris + [entity_linker_graph]
    #         )
    #     )
    # )
    loadQuery = (
        """  
  PREFIX owl: <http://www.w3.org/2002/07/owl#> 
  CONSTRUCT {
    ?s owl:sameAs ?o
  }
  """
        # + from_part
        + """
  WHERE {
    VALUES ?class {<"""
        + entity_source_type
        + """> <"""
        + entity_source_proxy_type
        + """>}
    ?s owl:sameAs ?o ;
       a ?class .
  }
  """
    )
    logger.info(loadQuery)

    # sparql.setQuery(loadQuery)
    # results = sparql.queryAndConvert()

    # with open("/tmp/lg_data.ttl", "w") as file:
    #     file.write(results.serialize())

    # g = Graph()
    # g.parse("/tmp/lg_data.ttl")
    g = sparql.query(loadQuery).graph

    logger.info("Linking data loaded")
    logger.info("sameAs statements: " + str(len(g)))
    return g


# get provided entities that already exist in source data (with entity proxies of which none has sameAs links to external sources)
@task
def get_existing_provided_entities_with_unmapped_proxies(
    sparql,
    entity_source_uris,
    entity_linker_graph,
    provided_entity_type,
    entity_proxy_for_property,
):
    logger = get_run_logger()

    # from_part = "\n".join(
    #     list(
    #         map(
    #             lambda x: "from <" + x + ">", entity_source_uris + [entity_linker_graph]
    #         )
    #     )
    # )
    entityQuery = (
        """
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    SELECT *
    """
        # + from_part
        + """
    WHERE {
      ?provided a <"""
        + provided_entity_type
        + """> .
      ?proxy <"""
        + entity_proxy_for_property
        + """> ?provided .
      FILTER NOT EXISTS {
        ?proxy2 <"""
        + entity_proxy_for_property
        + """> ?provided ;
                owl:sameAs ?ext_id .
        GRAPH <"""
        + entity_linker_graph
        + """> {
          ?proxy3 owl:sameAs ?ext_id .
        }
      }
    }
    """
    )
    logger.info(entityQuery)

    # sparql.setQuery(entityQuery)
    # sparql.setReturnFormat(JSON)
    results = sparql.query(entityQuery)

    provided_entities = dict()
    for result in results.bindings:
        provided_entity = result["provided"].toPython()
        entity_proxy = result["proxy"].toPython()
        if provided_entity not in provided_entities:
            provided_entities[provided_entity] = list()
        provided_entities[provided_entity].append(entity_proxy)

    logger.info("Queried existing provided entities with no mapped proxy entities")
    logger.info("provided entity count: " + str(len(provided_entities)))
    return provided_entities


# get entity proxies that don't have sameAs links to external sources (and aren't connected to existing provided entities in source data)
@task
def get_unmapped_proxies_without_existing_provided_entities(
    sparql,
    entity_source_uris,
    entity_linker_graph,
    entity_source_type,
    entity_source_proxy_type,
    entity_proxy_for_property,
):
    logger = get_run_logger()

    # from_part = "\n".join(
    #     list(
    #         map(
    #             lambda x: "from <" + x + ">", entity_source_uris + [entity_linker_graph]
    #         )
    #     )
    # )
    entityQuery = (
        """
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    SELECT DISTINCT ?entity
    """
        # + from_part
        + """
    WHERE {
      VALUES ?class {<"""
        + entity_source_type
        + """> <"""
        + entity_source_proxy_type
        + """>}
      ?entity a ?class .
      FILTER NOT EXISTS {
        ?entity <"""
        + entity_proxy_for_property
        + """> ?provided ;
      }
      FILTER NOT EXISTS {
        ?entity owl:sameAs ?ext_id .
        GRAPH <"""
        + entity_linker_graph
        + """> {
          ?entity2 owl:sameAs ?ext_id .
        }
      }
    }
    """
    )
    logger.info(entityQuery)

    # sparql.setQuery(entityQuery)
    # sparql.setReturnFormat(JSON)
    results = sparql.query(entityQuery)

    entity_proxies = list()
    for result in results.bindings:
        entity_proxies.append(result["entity"].toPython())

    logger.info("Queried unmapped entity proxies without existing provided entities")
    logger.info("entity proxy count: " + str(len(entity_proxies)))
    return entity_proxies


@task
def create_provided_entities_graph(
    sparql,
    id_graph,
    entity_enriched_uris,
    entity_source_type,
    entity_source_proxy_type,
    provided_entity_ns,
    provided_entity_type,
    entity_proxy_for_property,
    provided_entities,
    entity_proxies,
):
    logger = get_run_logger()

    # 1) entities that have sameAs links to external sources (e.g. Wikidata)
    entityQuery = (
        """
  SELECT DISTINCT ?entity
  WHERE
  {
    VALUES ?class {<"""
        + entity_source_type
        + """> <"""
        + entity_source_proxy_type
        + """>}
    ?entity a ?class
  } 
  """
    )

    providedEntityTypeURI = URIRef(provided_entity_type)
    entityProxyForPropertyURI = URIRef(entity_proxy_for_property)

    g = Graph()

    addedEntityProxies = set()

    providedEntityCount = 0
    # sparql.setQuery(entityQuery)
    # sparql.setReturnFormat(JSON)
    results = sparql.query(entityQuery)
    for result in results.bindings:
        entityProxyURI = result["entity"].toPython()
        check = False
        entityProxy = URIRef(entityProxyURI)
        providedEntityURI = URIRef(provided_entity_ns + str(providedEntityCount))
        g.add((providedEntityURI, RDF.type, providedEntityTypeURI))
        # check for owl:sameAs links
        added = False
        for extID in id_graph.objects(entityProxy, OWL.sameAs):
            for otherProxy in id_graph.subjects(OWL.sameAs, extID):
                if not otherProxy in addedEntityProxies:
                    g.add((otherProxy, entityProxyForPropertyURI, providedEntityURI))
                    addedEntityProxies.add(otherProxy)
                    added = True
        if not entityProxy in addedEntityProxies:
            g.add((entityProxy, entityProxyForPropertyURI, providedEntityURI))
            addedEntityProxies.add(entityProxy)
            added = True

        if added:
            providedEntityCount = providedEntityCount + 1

    logger.info("Number of provided entities created: " + str(providedEntityCount))

    # 2a) existing provided entities with entity proxies of which none has sameAs links to external sources
    for entityProxies in provided_entities.values():
        providedEntityURI = URIRef(provided_entity_ns + str(providedEntityCount))
        g.add((providedEntityURI, RDF.type, providedEntityTypeURI))
        for entityProxy in entityProxies:
            entityProxyURI = URIRef(entityProxy)
            g.add((entityProxyURI, entityProxyForPropertyURI, providedEntityURI))
        providedEntityCount = providedEntityCount + 1

    logger.info(
        "Number of provided entities created (incl. existing, non-mapped ones): "
        + str(providedEntityCount)
    )

    # 2b) entity proxies that don't have sameAs links to external sources and aren't connected to existing provided entities
    for entityProxy in entity_proxies:
        providedEntityURI = URIRef(provided_entity_ns + str(providedEntityCount))
        g.add((providedEntityURI, RDF.type, providedEntityTypeURI))
        entityProxyURI = URIRef(entityProxy)
        g.add((entityProxyURI, entityProxyForPropertyURI, providedEntityURI))
        providedEntityCount = providedEntityCount + 1

    logger.info(
        "Number of provided entities created (incl. for non-mapped proxies without existing ones): "
        + str(providedEntityCount)
    )

    # g.serialize(destination="providedEntities.ttl")

    logger.info("Entity provided updated")
    return g


@task()
def update_target_graph(endpoint, target_uri, data):
    # return
    logger = get_run_logger()
    delete_url = endpoint + "?c=<" + target_uri + ">"
    auth = HTTPBasicAuth(os.environ.get("RDFDB_USER"), os.environ.get("RDFDB_PASSWORD"))
    post_url = endpoint + "?context-uri=" + target_uri + ""
    res = requests.delete(delete_url, auth=auth)
    res2 = requests.post(
        post_url,
        headers={"Content-type": "text/turtle"},
        data=data.serialize().encode("utf-8"),
        auth=auth,
    )
    logger.info(res)
    logger.info(res2)
    logger.info(len(data))
    return True


@task()
def get_start_time():
    schedule_time = FlowRunContext.get()
    if hasattr(schedule_time, "scheduled_start_time"):
        return schedule_time.scheduled_start_time
    else:
        return datetime.datetime.now()


def create_source_entities():
    entities = []
    flow_run_id = prefect.context.flow_run_id
    params = prefect.context.parameters
    g = Graph()
    for index, source_graph in enumerate(params["entity_source_uris"]):
        source_entity = URIRef(IDM_PROV[flow_run_id + "/source/" + str(index)])

        g.add((source_entity, RDF.type, PROV.Entity))
        g.add((source_entity, IDM_PROV.source_graph, URIRef(source_graph)))
        entities.append(source_entity)

    # entity_enriched_uris
    if params["entity_enriched_uris"] != "":
        id_source = URIRef(IDM_PROV[flow_run_id + "/source/id_source"])
        g.add((id_source, RDF.type, PROV.Entity))
        g.add(
            (id_source, IDM_PROV.source_graph, URIRef(params["entity_enriched_uris"]))
        )
        entities.append(source_entity)

    return entities, g


def create_target_entities():
    entities = []
    flow_run_id = prefect.runtime.deployment.flow_run_id
    params = prefect.runtime.deployment.parameters
    g = Graph()

    target_graph = URIRef(IDM_PROV[flow_run_id + "/target"])
    g.add((target_graph, RDF.type, PROV.Entity))
    g.add((target_graph, IDM_PROV.target_graph, URIRef(params["target_graph"])))
    entities.append(target_graph)

    return entities, g


@task()
def add_provenance(
    _, start_time, create_source_entities, create_target_entities, endpoint
):
    # return
    logger = get_run_logger()

    flow_name = prefect.runtime.flow_run.flow_name
    # flow_id = prefect.runtime.flow_runid
    flow_run_id = prefect.runtime.flow_run.id
    flow_run_version = prefect.runtime.deployment.get("version", "not-available")
    end_time = datetime.datetime.now()

    g = Graph()

    activityURI = URIRef(IDM_PROV["activity/" + flow_run_id])
    g.add((activityURI, RDF.type, PROV.Activity))
    g.add((activityURI, IDM_PREFECT.flow_name, Literal(flow_name)))
    # g.add((activityURI, IDM_PREFECT.flow_id, Literal(flow_id)))
    g.add((activityURI, IDM_PREFECT.flow_run_version, Literal(flow_run_version)))
    g.add(
        (
            activityURI,
            PROV.startedAtTime,
            Literal(start_time.isoformat(), datatype=XSD.dateTime),
        )
    )
    g.add(
        (
            activityURI,
            PROV.endedAtTime,
            Literal(end_time.isoformat(), datatype=XSD.dateTime),
        )
    )
    # used
    source_entities, source_entity_graph = create_source_entities()
    g = g + source_entity_graph
    for source_entity in source_entities:
        g.add((activityURI, PROV.used, source_entity))

    # generated
    target_entities, target_entity_graph = create_target_entities()
    g = g + target_entity_graph
    for target_entity in target_entities:
        g.add((activityURI, PROV.generated, target_entity))

    return g


class Params(BaseModel):
    endpoint: HttpUrl = Field(
        "https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql",
        description="The SPARQL endpoint to query and update. Not needed if GitHub is used.",
    )
    entity_source_uris: List[HttpUrl] = Field(
        [
            "http://apis.acdh.oeaw.ac.at/data/v5",
            "http://ldf.fi/nbf/data",
            "http://data.biographynet.nl",
            "http://www.intavia.eu/sbi",
        ],
        description="The datasets to query for source URIs.",
    )
    entity_source_type: HttpUrl = Field(
        "http://www.cidoc-crm.org/cidoc-crm/E21_Person",
        description="The type of the source entities.",
    )
    entity_source_proxy_type: HttpUrl = Field(
        "http://www.intavia.eu/idm-core/Person_Proxy",
        description="The type of the source entity proxies.",
    )
    entity_enriched_uris: HttpUrl = Field(
        "http://www.intavia.eu/graphs/person-id-enrichment",
        description="The base URI for provided entities.",
    )
    provided_entity_ns: HttpUrl = Field(
        "http://www.intavia.eu/provided_person/",
        description="",
    )
    provided_entity_type: HttpUrl = Field(
        "http://www.intavia.eu/idm-core/Provided_Person", description=""
    )
    entity_proxy_for_property: HttpUrl = Field(
        "http://www.intavia.eu/idm-core/proxy_for"
    )
    target_graph: HttpUrl = Field("http://www.intavia.eu/graphs/provided_persons")
    github_repo: HttpUrl | None = Field(
        "https://github.com/InTaVia/source-data.git",
        description="The GitHub repository to use as source. Set to None to use the SPARQL endpoint.",
    )
    github_branch_source: str = Field(
        "main", description="The GitHub branch to use as source."
    )
    github_branch_target: str = Field(
        ..., description="The GitHub branch to use as target."
    )
    github_branch_target_provenance: str = Field(
        ..., description="The GitHub branch to use as target for the provenance graph."
    )
    use_github_as_target: bool = Field(
        True, description="Whether to push the created graph to GitHub."
    )
    storage_path: DirectoryPath = Field(
        "/archive/serializations/Provided_entities",
        description="Path of the turtle file to use for serialization within the container excuting the flow",
    )


@flow
def create_provided_entities_flow(params: Params):
    start_time = get_start_time()
    if params.github_repo is not None:
        sparql = create_conjunctive_graph_from_github_branch(
            params.github_repo,
            params.github_branch_source,
        )
        sparql2 = sparql
    else:
        sparql = setup_sparql_connection(params.endpoint)
        sparql2 = setup_sparql_connection(params.endpoint)
    id_graph = get_sameas_statements(
        sparql2,
        params.entity_source_uris,
        params.entity_enriched_uris,
        params.entity_source_type,
        params.entity_source_proxy_type,
    )
    provided_entities = get_existing_provided_entities_with_unmapped_proxies(
        sparql,
        params.entity_source_uris,
        params.entity_enriched_uris,
        params.provided_entity_type,
        params.entity_proxy_for_property,
    )
    entity_proxies = get_unmapped_proxies_without_existing_provided_entities(
        sparql,
        params.entity_source_uris,
        params.entity_enriched_uris,
        params.entity_source_type,
        params.entity_source_proxy_type,
        params.entity_proxy_for_property,
    )
    provided_entities_graph = create_provided_entities_graph(
        sparql,
        id_graph,
        params.entity_enriched_uris,
        params.entity_source_type,
        params.entity_source_proxy_type,
        params.provided_entity_ns,
        params.provided_entity_type,
        params.entity_proxy_for_property,
        provided_entities,
        entity_proxies,
    )
    if params.use_github_as_target:
        file_path = serialize_graph(
            provided_entities_graph,
            params.storage_path,
            "provided_entities_graph",
            True,
        )
        res = push_data_to_repo_flow(
            params=ParamsPush(
                branch_name=params.github_branch_target,
                branch_name_add_date=True,
                file_path_git="datasets/provided_entities_graph.ttl",
                commit_message="Updates provided entities graph",
                file_path=file_path,
            )
        )

    else:
        res = update_target_graph(
            params.endpoint, params.target_graph, provided_entities_graph
        )
    prov_graph = add_provenance(
        res, start_time, create_source_entities, create_target_entities, params.endpoint
    )
    if params.use_github_as_target:
        file_path = serialize_graph(
            prov_graph, params.storage_path, "provenance_graph", True
        )
        res = push_data_to_repo_flow(
            params=ParamsPush(
                branch_name=params.github_branch_target_provenance,
                branch_name_add_date=True,
                file_path=file_path,
                file_path_git="datasets/provenance_graph.ttl",
                commit_message="Updates provenance graph",
            )
        )

    else:
        update_target_graph(sparql, PROV_TARGET_GRAPH, prov_graph)


# flow.run_config = KubernetesRun(
#     env={"EXTRA_PIP_PACKAGES": "SPARQLWrapper rdflib requests"},
#     job_template_path="https://raw.githubusercontent.com/InTaVia/prefect-flows/master/intavia-job-template.yaml",
# )
# flow.storage = GitHub(repo="InTaVia/prefect-flows", path="update_provided_entities.py")

# default settings
# flow.run()

# Persons
# flow.run(
#    #endpoint='http://localhost:9999/blazegraph/namespace/intavia/sparql',
#    endpoint='http://localhost:9999/blazegraph/sparql',
#    #endpoint='https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql',
#    entity_source_uris=['http://apis.acdh.oeaw.ac.at/data/v5', 'http://ldf.fi/nbf/data', 'http://data.biographynet.nl', 'http://www.intavia.eu/sbi'],
#    entity_source_type="http://www.cidoc-crm.org/cidoc-crm/E21_Person",
#    entity_source_proxy_type="http://www.intavia.eu/idm-core/Person_Proxy",
#    entity_enriched_uris="http://www.intavia.org/graphs/person-id-enrichment",
#    provided_entity_ns="http://www.intavia.eu/provided_person/",
#    provided_entity_type="http://www.intavia.eu/idm-core/Provided_Person",
#    entity_proxy_for_property="http://www.intavia.eu/idm-core/proxy_for",
#    target_graph='http://www.intavia.eu/graphs/provided_persons'
# )

# Places
# flow.run(
#    #endpoint='http://localhost:9999/blazegraph/namespace/intavia/sparql',
#    endpoint='http://localhost:9999/blazegraph/sparql',
#    #endpoint='https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql',
#    entity_source_uris=['http://apis.acdh.oeaw.ac.at/data/v5', 'http://ldf.fi/nbf/data', 'http://data.biographynet.nl', 'http://data.biographynet.nl/places2wikidata/', 'http://www.intavia.eu/sbi'],
#    entity_source_type="http://www.cidoc-crm.org/cidoc-crm/E53_Place",
#    entity_source_proxy_type="http://www.intavia.eu/idm-core/Place_Proxy",
#    entity_enriched_uris="http://www.intavia.org/graphs/place-id-enrichment",
#    provided_entity_ns="http://www.intavia.eu/provided_place/",
#    provided_entity_type="http://www.intavia.eu/idm-core/Provided_Place",
#    entity_proxy_for_property="http://www.intavia.eu/idm-core/proxy_for",
#    target_graph='http://www.intavia.eu/graphs/provided_places'
# )

# Groups
# flow.run(
#    #endpoint='http://localhost:9999/blazegraph/namespace/intavia/sparql',
#    endpoint='http://localhost:9999/blazegraph/sparql',
#    #endpoint='https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql',
#    entity_source_uris=['http://apis.acdh.oeaw.ac.at/data/v5', 'http://ldf.fi/nbf/data', 'http://data.biographynet.nl', 'http://www.intavia.eu/sbi'],
#    entity_source_type="http://www.cidoc-crm.org/cidoc-crm/E74_Group",
#    entity_source_proxy_type="http://www.intavia.eu/idm-core/Group",
#    entity_enriched_uris="http://www.intavia.org/graphs/group-id-enrichment",
#    provided_entity_ns="http://www.intavia.eu/provided_group/",
#    provided_entity_type="http://www.intavia.eu/idm-core/Provided_Group",
#    entity_proxy_for_property="http://www.intavia.eu/idm-core/proxy_for",
#    target_graph='http://www.intavia.eu/graphs/provided_groups'
# )

# CHO's
# flow.run(
#    #endpoint='http://localhost:9999/blazegraph/namespace/intavia/sparql',
#    endpoint='http://localhost:9999/blazegraph/sparql',
#    #endpoint='https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql',
#    entity_source_uris=['http://data.acdh.oeaw.ac.at/intavia/cho/v6'],
#    entity_source_type="http://www.cidoc-crm.org/cidoc-crm/E24_Physical_Human_Made_Thing",
#    entity_source_proxy_type="http://www.intavia.eu/idm-core/CHO_Proxy",
#    entity_enriched_uris="",
#    provided_entity_ns="http://www.intavia.eu/provided_cho/",
#    provided_entity_type="http://www.intavia.eu/idm-core/Provided_CHO",
#    entity_proxy_for_property="http://www.intavia.eu/idm-core/proxy_for",
#    target_graph='http://www.intavia.eu/graphs/provided_cho'
# )
