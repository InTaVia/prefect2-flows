from collections import ChainMap
import enum
from itertools import chain
import os
from pathlib import Path
import shutil
import git
from prefect import flow, get_run_logger, task, unmapped
from copy import deepcopy
from datetime import datetime, timedelta
import requests
from rdflib import Graph, Literal, RDF, Namespace, URIRef
from rdflib.namespace import RDFS, XSD
from typing import Any, Tuple
from pydantic import BaseModel, DirectoryPath, Field, HttpUrl
from push_rdf_file_to_github import push_data_to_repo_flow
from push_rdf_file_to_github import Params as ParamsPush

# from prefect.engine.signals import LOOP, SUCCESS, SKIP, RETRY, FAIL

BASE_URL_API = "http://localhost:5000/apis/api"
BASE_URI_SERIALIZATION = "https://apis.acdh.oeaw.ac.at/"


class EntityTypeEnum(enum.Enum):
    person = "person"
    institution = "institution"
    place = "place"
    work = "work"
    event = "event"


class APIEndpointsEnum(enum.Enum):
    relations = "relations"
    entities = "entities"


def convert_timedelta(duration):
    days, seconds = duration.days, duration.seconds
    hours = days * 24 + seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return hours, minutes, seconds


def create_time_span_tripels(kind, event_node, obj, g):
    if kind == "start":
        if obj["start_date_written"] is not None:
            if len(obj[f"start_date_written"]) > 0:
                label_date = obj[f"start_date_written"]
                if obj["end_date_written"] is not None:
                    if len(obj["end_date_written"]) > 0:
                        label_date += f" - {obj['end_date_written']}"
                g.add((event_node, rdfs.label, Literal(label_date)))
    if len(obj[f"{kind}_date_written"]) == 4:
        # check whether only a year has bin given for the start date and add according nodes
        if kind == "start":
            g.add(
                (
                    event_node,
                    crm.P82a_begin_of_the_begin,
                    (
                        Literal(
                            f"{obj[f'{kind}_date_written']}-01-01T00:00:00",
                            datatype=XSD.dateTime,
                        )
                    ),
                )
            )
            g.add(
                (
                    event_node,
                    crm.P81a_end_of_the_begin,
                    (
                        Literal(
                            f"{obj[f'{kind}_date_written']}-12-31T23:59:59",
                            datatype=XSD.dateTime,
                        )
                    ),
                )
            )
        elif kind == "end":
            g.add(
                (
                    event_node,
                    crm.P82b_end_of_the_end,
                    (
                        Literal(
                            f"{obj[f'{kind}_date_written']}-12-31T23:59:59",
                            datatype=XSD.dateTime,
                        )
                    ),
                )
            )
            g.add(
                (
                    event_node,
                    crm.P81b_begin_of_the_end,
                    (
                        Literal(
                            f"{obj[f'{kind}_date_written']}-01-01T00:00:00",
                            datatype=XSD.dateTime,
                        )
                    ),
                )
            )
    else:
        if kind == "start":
            g.add(
                (
                    event_node,
                    crm.P82a_begin_of_the_begin,
                    (Literal(f"{obj[f'{kind}_date']}T00:00:00", datatype=XSD.dateTime)),
                )
            )
        elif kind == "end":
            g.add(
                (
                    event_node,
                    crm.P82b_end_of_the_end,
                    (Literal(f"{obj[f'{kind}_date']}T23:59:59", datatype=XSD.dateTime)),
                )
            )
    g.add((event_node, RDF.type, crm["E52_Time-Span"]))
    return g


@task()
def render_personplace_relation(rel, g, base_uri):
    """renders personplace relation as RDF graph

    Args:
        pers_uri (_type_): _description_
        rel (_type_): _description_
        g (_type_): _description_
    """
    # prepare nodes
    place = None
    place_uri = idmapis[f"place.{rel['related_place']['id']}"]
    if rel["relation_type"]["id"] == 595:
        # define serialization for "person born in place relations"
        g.add(
            (
                idmapis[f"birthevent.{rel['related_person']['id']}"],
                crm.P7_took_place_at,
                place_uri,
            )
        )
        rel_rendered = True
    elif rel["relation_type"]["id"] == 596:
        # define serialization for "person born in place relations"
        g.add(
            (
                idmapis[f"deathevent.{rel['related_person']['id']}"],
                crm.P7_took_place_at,
                place_uri,
            )
        )
        rel_rendered = True
    else:
        event_uri = idmapis[f"event.personplace.{rel['id']}"]
        if (event_uri, None, None) not in g:
            g = render_event(rel, "personplace", event_uri, g)
        g.add((event_uri, crm.P7_took_place_at, place_uri))
        rel_rendered = True

    if (
        rel_rendered
        and (place_uri, None, None) not in g
        and rel["related_place"]["id"] not in glob_list_entities["places"]
    ):
        place = rel["related_place"]["id"]
        glob_list_entities["places"].append(rel["related_place"]["id"])
    return place


@task()
def render_personperson_relation(rel, g, base_uri):
    """renders personperson relation as RDF graph

    Args:
        pers_uri (_type_): _description_
        rel (_type_): _description_
        g (_type_): _description_http://localhost:8000/apis/api/entities/person/
    """
    # prepare nodes
    logger = get_run_logger()
    family_relations = [
        5870,  # war Bruder von
        5871,  # war Schwester von
        5741,  # family member
        5414,  # war Kind von
        5413,  # war Elternteil von
        5412,  # war verwandt
        5411,  # war verheiratet
    ]
    if isinstance(rel, list):
        if len(rel) == 0:
            logger.warning("No person-person relations found skipping rest of task")
            return None
            # return Exception("No person-person relations found skipping")
    if rel["relation_type"]["label"] == "undefined":
        logger.warning("Relation type is undefined, skipping rest of task")
        # return Exception("Relation type is undefined, skipping")
    person = None
    pers_uri = idmapis[f"personproxy.{rel['related_personA']['id']}"]
    n_rel_type = idmapis[f"personrelation.{rel['id']}"]
    n_relationtype = idmrelations[f"{rel['relation_type']['id']}"]
    if rel["relation_type"]["id"] in family_relations:
        g.add((pers_uri, bioc.has_family_relation, n_rel_type))
    else:
        g.add((pers_uri, bioc.has_person_relation, n_rel_type))
    g.add((n_rel_type, RDF.type, n_relationtype))
    g.add((n_rel_type, RDFS.label, Literal(f"{rel['relation_type']['label']}")))
    g.add(
        (
            idmapis[f"personproxy.{rel['related_personB']['id']}"],
            bioc.bearer_of,
            n_rel_type,
        )
    )
    if rel["related_personB"]["id"] not in glob_list_entities["persons"]:
        glob_list_entities["persons"].append(rel["related_personB"]["id"])
        person = rel["related_personB"]["id"]
    # g.add(n_rel_type, bioc.bearer_of, (URIRef(
    #    f"{idmapis}personproxy/{rel['related_personB']['id']}")))
    # TODO: add hiarachy of person relations
    if rel["relation_type"] is not None:
        if rel["relation_type"]["parent_id"] is not None:
            g.add(
                (
                    n_relationtype,
                    RDFS.subClassOf,
                    idmrelations[f"{rel['relation_type']['parent_id']}"],
                )
            )
            if rel["relation_type"]["id"] in family_relations:
                g.add(
                    (
                        idmrelations[f"{rel['relation_type']['parent_id']}"],
                        RDFS.subClassOf,
                        bioc.Family_Relationship_Role,
                    )
                )
            else:
                g.add(
                    (
                        idmrelations[f"{rel['relation_type']['parent_id']}"],
                        RDFS.subClassOf,
                        bioc.Person_Relationship_Role,
                    )
                )
        else:
            if rel["relation_type"]["id"] in family_relations:
                g.add(
                    (
                        idmrelations[f"{rel['relation_type']['id']}"],
                        RDFS.subClassOf,
                        bioc.Family_Relationship_Role,
                    )
                )
            else:
                g.add(
                    (
                        idmrelations[f"{rel['relation_type']['id']}"],
                        RDFS.subClassOf,
                        bioc.Person_Relationship_Role,
                    )
                )

    else:
        if rel["relation_type"]["id"] in family_relations:
            g.add((n_relationtype, RDFS.subClassOf, bioc.Family_Relationship_Role))
        else:
            g.add((n_relationtype, RDFS.subClassOf, bioc.Person_Relationship_Role))
    logger.info(f" personpersonrelation serialized for: {rel['related_personA']['id']}")
    # return g
    # group which is part of this relation
    return person


@task()
def render_personrole_from_relation(rel, g, base_uri):
    """renders personrole as RDF graph

    Args:
        pers_uri (_type_): _description_
        role (_type_): _description_
        g (_type_): _description_
    """
    # prepare nodes
    logger = get_run_logger()
    if (
        idmapis[f"personrole.{rel['relation_type']['id']}"],
        None,
        None,
    ) not in g:
        for k in rel.keys():
            if k.startswith("related_"):
                if k.split("_")[1] != "person":
                    role = rel["relation_type"]
                    second_entity = k.split("_")[1]
    else:
        logger.info("personrole already in graph")
        return None
    label = "label"
    if "label" not in role:
        label = "name"
    parent = "parent_id"
    if "parent_id" not in role:
        parent = "parent_class"
    n_role = idmapis[f"personrole.{role['id']}"]
    g.add((n_role, RDFS.label, Literal(f"{role[label]}", lang="de")))
    if role[parent] is not None:
        if "parent_id" in role:
            p_id = role["parent_id"]
        else:
            p_id = role["parent_class"]["id"]
        if (idmapis[f"personrole.{p_id}"], None, None) not in g:
            return f"{base_uri}/apis/api/vocabularies/person{second_entity}relation/{role['parent_id']}"
    else:
        g.add((n_role, RDF.type, bioc.Actor_Role))
    return None


@task
def render_personrole(role, g, base_uri):
    logger = get_run_logger()
    if role is None:
        logger.warning("No role given")
        return None
    if (idmapis[f"personrole.{role['id']}"], None, None) in g:
        logger.info("personrole already in graph")
        return None
    n_role = idmapis[f"personrole.{role['id']}"]
    g.add((n_role, RDFS.label, Literal(f"{role['name']}", lang="de")))
    if role["parent_class"] is not None:
        if (
            idmapis[f"personrole.{role['parent_class']['id']}"],
            None,
            None,
        ) not in g:
            logger.info("parent role not in graph")
            return role["parent_class"]["id"]
    else:
        g.add((n_role, RDF.type, bioc.Actor_Role))
        return None


@task
def render_personinstitution_relation(rel: dict, g: Graph) -> list:
    logger = get_run_logger()
    pers_uri = idmapis[f"personproxy.{rel['related_person']['id']}"]
    inst = None
    # connect personproxy and institutions with grouprelationship
    n_rel_type = idmapis[f"grouprelation.{rel['id']}"]
    g.add((pers_uri, bioc.has_group_relation, n_rel_type))
    # Person has a specific group relation
    g.add(
        (
            n_rel_type,
            RDF.type,
            idmapis[f"grouprole.{rel['relation_type']['id']}"],
        )
    )
    # define type of grouprelation
    if rel["relation_type"]["parent_id"] is not None:
        # if the relationtype has a superclass, it is added here
        g.add(
            (
                idmapis[f"grouprole.{rel['relation_type']['id']}"],
                rdfs.subClassOf,
                idmapis[f"grouprole.{rel['relation_type']['parent_id']}"],
            )
        )
    g.add((n_rel_type, rdfs.label, Literal(rel["relation_type"]["label"])))
    # add label to relationtype
    g.add(
        (
            n_rel_type,
            bioc.inheres_in,
            idmapis[f"groupproxy.{rel['related_institution']['id']}"],
        )
    )
    # g.add((URIRef(
    #    f"{idmapis}groupproxy/{rel['related_institution']['id']}"), bioc.bearer_of, n_rel_type))
    # group which is part of this relation
    g.add((idmapis[f"career.{rel['id']}"], RDF.type, idmcore.Career))
    # add career event of type idmcore:career
    g.add(
        (
            idmcore.Career,
            rdfs.subClassOf,
            crm.E5_Event,
        )
    )
    g.add(
        (
            idmapis[f"career.{rel['id']}"],
            rdfs.label,
            Literal(
                f"{rel['related_person']['label']} {rel['relation_type']['label']} {rel['related_institution']['label']}"
            ),
        )
    )
    # label for career event
    g.add(
        (
            idmapis[f"career.{rel['id']}"],
            bioc.had_participant_in_role,
            idmapis[f"personrole.{rel['id']}.{rel['related_person']['id']}"],
        )
    )
    # role of participating person in the career event
    g.add(
        (
            idmapis[f"personproxy.{rel['related_person']['id']}"],
            bioc.bearer_of,
            idmapis[f"personrole.{rel['id']}.{rel['related_person']['id']}"],
        )
    )
    g.add(
        (
            idmapis[f"personrole.{rel['id']}.{rel['related_person']['id']}"],
            RDF.type,
            idmapis[f"personrole.{rel['relation_type']['id']}"],
        )
    )
    if rel["relation_type"]["parent_id"] is not None:
        g.add(
            (
                idmapis[f"personrole.{rel['relation_type']['id']}"],
                RDF.type,
                idmapis[f"personrole.{rel['relation_type']['parent_id']}"],
            )
        )
    # person which inheres this role
    g.add(
        (
            idmapis[f"career.{rel['id']}"],
            bioc.had_participant_in_role,
            idmapis[f"grouprole.{rel['id']}.{rel['related_institution']['id']}"],
        )
    )
    # role of institution/ group in the career event
    g.add(
        (
            idmapis[f"grouprole.{rel['id']}.{rel['related_institution']['id']}"],
            RDF.type,
            bioc.Group_Relationship_Role,
        )
    )
    g.add(
        (
            idmapis[f"grouprole.{rel['id']}.{rel['related_institution']['id']}"],
            bioc.inheres_in,
            idmapis[f"groupproxy.{rel['related_institution']['id']}"],
        )
    )
    # role which inheres the institution/ group
    if (
        idmapis[f"groupproxy.{rel['related_institution']['id']}"],
        None,
        None,
    ) not in g:
        if rel["related_institution"]["id"] not in glob_list_entities["institutions"]:
            inst = rel["related_institution"]["id"]
            glob_list_entities["institutions"].append(rel["related_institution"]["id"])
    if rel["start_date"] is not None or rel["end_date"] is not None:
        g.add(
            (
                idmapis[f"career.{rel['id']}"],
                URIRef(f"{crm}P4_has_time-span"),
                idmapis[f"career.timespan.{rel['id']}"],
            )
        )
    for rel_plcs in g.objects(
        idmapis[f"groupproxy.{rel['related_institution']['id']}"],
        crm.P74_has_current_or_former_residence,
    ):
        g.add((idmapis[f"career.{rel['id']}"], crm.P7_took_place_at, rel_plcs))
    logger.info(
        f" personinstitutionrelation serialized for: {rel['related_person']['id']}"
    )
    if rel["start_date"] is not None:
        g = create_time_span_tripels(
            "start", idmapis[f"career.timespan.{rel['id']}"], rel, g
        )
    if rel["end_date"] is not None:
        g = create_time_span_tripels(
            "end", idmapis[f"career.timespan.{rel['id']}"], rel, g
        )
    """     if (rel['start_date'] is not None) and (rel['end_date'] is not None):
        g.add((URIRef(f"{idmapis}career/timespan/{rel['id']}"), crm.P82a_begin_of_the_begin, (Literal(
            rel['start_date']+'T00:00:00', datatype=XSD.dateTime))))
        g.add((URIRef(f"{idmapis}career/timespan/{rel['id']}"), crm.P82b_end_of_the_end, (Literal(
            rel['end_date']+'T23:59:59', datatype=XSD.dateTime))))
        g.add((URIRef(f"{idmapis}career/timespan/{rel['id']}"), rdfs.label, Literal(
            rel['start_date_written'])+' - ' + rel['end_date_written']))
    elif ((rel['start_date'] is not None) and (rel['end_date'] is not None)):
        g.add((URIRef(f"{idmapis}career/timespan/{rel['id']}"), crm.P82a_begin_of_the_begin, (Literal(
            rel['start_date']+'T00:00:00', datatype=XSD.dateTime))))
        g.add((URIRef(
            f"{idmapis}career/timespan/{rel['id']}"), rdfs.label, Literal(rel['start_date_written'])))
    elif ((rel['start_date'] is not None) and (rel['end_date'] is not None)):
        g.add((URIRef(f"{idmapis}career/timespan/{rel['id']}"), crm.P82b_end_of_the_end, (Literal(
            rel['end_date']+'T23:59:59', datatype=XSD.dateTime))))
        g.add((URIRef(f"{idmapis}career/timespan/{rel['id']}"), rdfs.label, Literal(
            'time-span end:' + rel['end_date_written']))) """
    return inst


@task()
def render_person(person, g, base_uri):
    """renders person object as RDF graph

    Args:
        person (_type_): _description_
        g (_type_): _description_
    """
    # pers_uri = URIRef(idmapis + f"personproxy/{person['id']}")
    pers_uri = idmapis[f"personproxy.{person['id']}"]
    if (pers_uri, None, None) in g:
        return g
    g.add((pers_uri, RDF.type, crm.E21_Person))
    g.add((pers_uri, RDF.type, idmcore.Person_Proxy))
    g.add((pers_uri, RDFS.label, Literal(f"{person['first_name']} {person['name']}")))
    # define that individual in APIS named graph and APIS entity are the same
    g.add((pers_uri, owl.sameAs, URIRef(f"{base_uri}/entity/{person['id']}")))
    # add sameAs
    # add appellations
    node_main_appellation = idmapis[f"appellation.label.{person['id']}"]
    g.add((node_main_appellation, RDF.type, crm.E33_E41_Linguistic_Appellation))
    g.add(
        (
            node_main_appellation,
            RDFS.label,
            Literal(
                f"{person['name'] if person['name'] is not None else '-'}, {person['first_name'] if person['first_name'] is not None else '-'}"
            ),
        )
    )
    g.add((pers_uri, crm.P1_is_identified_by, node_main_appellation))
    if person["first_name"] is not None:
        node_first_name_appellation = idmapis[f"appellation.first_name.{person['id']}"]
        g.add(
            (node_first_name_appellation, RDF.type, crm.E33_E41_Linguistic_Appellation)
        )
        g.add((node_first_name_appellation, RDFS.label, Literal(person["first_name"])))
        g.add(
            (node_main_appellation, crm.P148_has_component, node_first_name_appellation)
        )
    if person["name"] is not None:
        node_last_name_appellation = idmapis[f"appellation.last_name.{person['id']}"]
        g.add(
            (node_last_name_appellation, RDF.type, crm.E33_E41_Linguistic_Appellation)
        )
        g.add((node_last_name_appellation, RDFS.label, Literal(person["name"])))
        g.add(
            (node_main_appellation, crm.P148_has_component, node_last_name_appellation)
        )
    if person["start_date"] is not None:
        node_birth_event = idmapis[f"birthevent.{person['id']}"]
        node_role = idmapis[f"born_person.{person['id']}"]
        node_role_class = idmrole[f"born_person"]
        node_time_span = idmapis[f"birth.timespan.{person['id']}"]
        g.add((node_role, bioc.inheres_in, pers_uri))
        g.add((node_role, RDF.type, node_role_class))
        g.add((node_role_class, rdfs.subClassOf, bioc.Event_Role))
        g.add((node_birth_event, bioc.had_participant_in_role, node_role))
        g.add((node_birth_event, RDF.type, crm.E67_Birth))
        g.add(
            (
                node_birth_event,
                RDFS.label,
                Literal(f"Birth of {person['first_name']} {person['name']}"),
            )
        )
        g.add((node_birth_event, crm["P4_has_time-span"], node_time_span))
        g.add((node_birth_event, crm.P98_brought_into_life, pers_uri))
        g = create_time_span_tripels("start", node_time_span, person, g)
    if person["end_date"] is not None:
        node_death_event = idmapis[f"deathevent.{person['id']}"]
        node_role = idmapis[f"deceased_person.{person['id']}"]
        node_role_class = idmrole["deceased_person"]
        node_time_span = idmapis[f"death.timespan.{person['id']}"]
        g.add((node_role, bioc.inheres_in, pers_uri))
        g.add((node_role, RDF.type, node_role_class))
        g.add((node_role_class, rdfs.subClassOf, bioc.Event_Role))
        g.add((node_death_event, bioc.had_participant_in_role, node_role))
        g.add((node_death_event, RDF.type, crm.E69_Death))
        g.add(
            (
                node_death_event,
                RDFS.label,
                Literal(f"Death of {person['first_name']} {person['name']}"),
            )
        )
        g.add((node_death_event, crm["P4_has_time-span"], node_time_span))
        g.add((node_death_event, crm.P100_was_death_of, pers_uri))
        g = create_time_span_tripels("end", node_time_span, person, g)
    for prof in person["profession"]:
        prof_node = idmapis[f"occupation.{prof['id']}"]
        g.add((pers_uri, bioc.has_occupation, prof_node))
        g.add((prof_node, rdfs.label, Literal(prof["label"])))
        if prof["parent_id"] is not None:
            parent_prof_node = idmapis[f"occupation.{prof['parent_id']}"]
            g.add((prof_node, rdfs.subClassOf, parent_prof_node))
            g.add((prof_node, rdfs.subClassOf, bioc.Occupation))
        else:
            g.add((prof_node, rdfs.subClassOf, bioc.Occupation))
    if person["gender"] is not None:
        g.add((pers_uri, bioc.has_gender, bioc[person["gender"].capitalize()]))
    for uri in person["sameAs"]:
        g.add((pers_uri, owl.sameAs, URIRef(uri)))
    if "text" in person:
        if len(person["text"]) > 1:
            g.add(
                (
                    pers_uri,
                    idmcore.bio_link,
                    idmapis[f"text.{person['id']}.bio"],
                )
            )
            g.add(
                (
                    idmapis[f"text.{person['id']}.bio"],
                    idmcore.full_bio_link,
                    URIRef(person["text"][0]["url"]),
                )
            )
            g.add(
                (
                    idmapis[f"text.{person['id']}.bio"],
                    idmcore.short_bio_link,
                    URIRef(person["text"][1]["url"]),
                )
            )
    # add occupations

    #    person_rel = await get_person_relations(person['id'], kinds=['personinstitution', 'personperson', 'personplace'])
    # tasks = []
    # for rel_type, rel_list in person_rel.items():
    #     if rel_type == 'personinstitution':
    #         for rel in rel_list:
    #             tasks.append(asyncio.create_task(
    #                 render_personinstitution_relation(pers_uri, rel, g)))
    #     elif rel_type == 'personperson':
    #         for rel in rel_list:
    #             tasks.append(asyncio.create_task(
    #                 render_personperson_relation(pers_uri, rel, g)))
    #     elif rel_type == 'personplace':
    #         for rel in rel_list:
    #             tasks.append(asyncio.create_task(
    #                 render_personplace_relation(pers_uri, rel, g)))
    #   await asyncio.gather(*tasks)
    return g


@task()
def render_organizationplace_relation(rel, g):
    place = None
    node_org = idmapis[f"groupproxy.{rel['related_institution']['id']}"]
    if (
        idmapis[f"place.{rel['related_place']['id']}"],
        None,
        None,
    ) not in g and rel[
        "related_place"
    ]["id"] not in glob_list_entities["places"]:
        place = rel["related_place"]["id"]
        glob_list_entities["places"].append(place)
    g.add(
        (
            node_org,
            crm.P74_has_current_or_former_residence,
            idmapis[f"place.{rel['related_place']['id']}"],
        )
    )
    return place


@task()
def render_organization(organization, g, base_uri):
    """renders organization object as RDF graph

    Args:
        organization (_type_): _description_
        g (_type_): _description_
    """
    #    res = await get_entity(organization, "institution")
    #    res_relations = await get_organization_relations(organization, ["institutionplace"])
    # setup basic nodes
    node_org = idmapis[f"groupproxy.{organization['id']}"]
    appelation_org = idmapis[f"groupappellation.{organization['id']}"]
    # connect Group Proxy and person in named graphbgn:BioDes
    g.add((node_org, RDF.type, crm.E74_Group))
    g.add((node_org, RDF.type, idmcore.Group))
    # defines group class
    g.add((node_org, owl.sameAs, URIRef(f"{base_uri}/entity/{organization['id']}")))
    for uri in organization["sameAs"]:
        g.add((node_org, owl.sameAs, URIRef(uri)))
    # defines group as the same group in the APIS dataset
    g.add((node_org, crm.P1_is_identified_by, appelation_org))
    g.add((appelation_org, rdfs.label, Literal(organization["name"])))
    g.add((appelation_org, RDF.type, crm.E33_E41_Linguistic_Appellation))
    # add group appellation and define it as linguistic appellation
    if organization["start_date_written"] is not None:
        if len(organization["start_date_written"]) >= 4:
            start_date_node = idmapis[f"groupstart.{organization['id']}"]
            start_date_time_span = idmapis[f"groupstart.timespan.{organization['id']}"]
            # print(row['institution_name'], ':', row['institution_start_date'], row['institution_end_date'], row['institution_start_date_written'], row['institution_end_date_written'])
            g.add((start_date_node, RDF.type, crm.E63_Beginning_of_Existence))
            g.add((start_date_node, crm.P92_brought_into_existence, node_org))
            if organization["start_date"] is not None:
                g.add(
                    (
                        start_date_node,
                        URIRef(crm + "P4_has_time-span"),
                        start_date_time_span,
                    )
                )
                g = create_time_span_tripels(
                    "start", start_date_time_span, organization, g
                )
            # if len(res['start_date_written']) == 4 and res['start_end_date'] is not None:
            #     # check whether only a year has bin given for the start date and add according nodes
            #     g.add((start_date_time_span, crm.P82a_begin_of_the_begin, (Literal(
            #         f"{res['start_start_date']}T00:00:00", datatype=XSD.dateTime))))
            #     g.add((start_date_time_span, crm.P82b_end_of_the_end, (Literal(
            #         f"{res['start_end_date']}T23:59:59", datatype=XSD.dateTime))))
            # else:
            #     g.add((start_date_time_span, crm.P82a_begin_of_the_begin, (Literal(
            #         f"{res['start_date']}T00:00:00", datatype=XSD.dateTime))))
            #     g.add((start_date_time_span, crm.P82b_end_of_the_end, (Literal(
            #         f"{res['start_date']}T23:59:59", datatype=XSD.dateTime))))
    if organization["end_date_written"] is not None:
        if len(organization["end_date_written"]) >= 4:
            end_date_node = idmapis[f"groupend.{organization['id']}"]
            end_date_time_span = idmapis[f"groupend.timespan.{organization['id']}"]
            # print(row['institution_name'], ':', row['institution_start_date'], row['institution_end_date'], row['institution_start_date_written'], row['institution_end_date_written'])
            g.add((end_date_node, RDF.type, crm.E64_End_of_Existence))
            g.add((end_date_node, crm.P93_took_out_of_existence, node_org))
            if organization["end_date"] is not None:
                g.add(
                    (
                        end_date_node,
                        URIRef(crm + "P4_has_time-span"),
                        end_date_time_span,
                    )
                )
                g = create_time_span_tripels("end", end_date_time_span, organization, g)
            # if len(res['end_date_written']) == 4 and res['end_end_date'] is not None:
            #     # check whether only a year has bin given for the start date and add according nodes
            #     g.add((end_date_time_span, crm.P82a_begin_of_the_begin, (Literal(
            #         f"{res['end_start_date']}T00:00:00", datatype=XSD.dateTime))))
            #     g.add((end_date_time_span, crm.P82b_end_of_the_end, (Literal(
            #         f"{res['end_end_date']}T23:59:59", datatype=XSD.dateTime))))
            # else:
            #     g.add((end_date_time_span, crm.P82a_begin_of_the_begin, (Literal(
            #         f"{res['end_date']}T00:00:00", datatype=XSD.dateTime))))
            #     g.add((end_date_time_span, crm.P82b_end_of_the_end, (Literal(
            #         f"{res['end_date']}T23:59:59", datatype=XSD.dateTime))))
    return g


def render_event(event, event_type, node_event, g):
    """renders event object as RDF graph

    Args:
        event (_type_): _description_
        g (_type_): _description_
    """
    # prepare basic node types
    # node_event = URIRef(f"{idmapis}{event_type}/{event['id']}")
    node_event_role = idmapis[f"{event_type}.eventrole.{event['id']}"]
    node_pers = idmapis[f"personproxy.{event['related_person']['id']}"]
    node_roletype = idmrole[f"{event['relation_type']['id']}"]
    g.add((node_event_role, bioc.inheres_in, node_pers))
    g.add((node_event_role, RDF.type, node_roletype))
    g.add((node_roletype, rdfs.subClassOf, bioc.Event_Role))
    g.add((node_roletype, RDFS.label, Literal(event["relation_type"]["label"])))
    # suggestion to add specific event role
    g.add((node_event, bioc.had_participant_in_role, node_event_role))
    # connect event and event role
    g.add((node_event, RDF.type, crm.E5_Event))
    # define crm classification
    g.add((node_event_role, RDFS.label, Literal(event["relation_type"]["label"])))
    g.add(
        (
            node_event,
            RDFS.label,
            Literal(
                f"{event['related_person']['label']} {event['relation_type']['label']} {event['related_place']['label']}"
            ),
        )
    )
    if event["start_date"] is not None:
        node_timespan = idmapis[f"{event_type}.timespan.{event['id']}"]
        g.add((node_event, URIRef(crm + "P4_has_time-span"), node_timespan))
        # add time-span to event
        g = create_time_span_tripels("start", node_timespan, event, g)
        # add end of time-span
        if event["end_date"] is not None:
            g = create_time_span_tripels("end", node_timespan, event, g)
    return g


@task()
def render_place(place, g, base_uri):
    """renders place object as RDF graph

    Args:
        place (_type_): _description_
        g (_type_): _description_
    """
    #    res = await get_entity(place, 'place')
    # setup basic nodes
    if place is None:
        return g
    node_place = idmapis[f"place.{place['id']}"]
    g.add((node_place, RDFS.label, Literal(place["name"])))
    node_appelation = idmapis[f"placeappellation.{place['id']}"]
    node_plc_identifier = idmapis[f"placeidentifier.{place['id']}"]

    g.add((node_place, RDF.type, crm.E53_Place))
    # define place as Cidoc E53 Place
    g.add((node_place, crm.P1_is_identified_by, node_appelation))
    # add appellation to place
    g.add((node_appelation, RDF.type, crm.E33_E41_Linguistic_Appellation))
    # define appellation as linguistic appellation
    g.add((node_appelation, RDFS.label, Literal(place["name"])))
    # add label to appellation
    g.add((node_place, owl.sameAs, URIRef(f"{base_uri}/entity/{place['id']}")))
    for uri in place["sameAs"]:
        g.add((node_place, owl.sameAs, URIRef(uri)))
    g.add(
        (
            node_place,
            crm.P1_is_identified_by,
            idmapis[f"placeidentifier.{place['id']}"],
        )
    )
    # add APIS Identifier as Identifier
    g.add((node_plc_identifier, RDF.type, crm.E_42_Identifier))
    # define APIS Identifier as E42 Identifier (could add a class APIS-Identifier or model a Identifier Assignment Event)
    g.add((node_plc_identifier, RDFS.label, Literal(place["id"])))
    # add label to APIS Identifier
    # define that individual in APIS named graph and APIS entity are the same
    if place["lat"] is not None and place["lng"] is not None:
        node_spaceprimitive = idmapis[f"spaceprimitive.{place['id']}"]
        g.add((node_place, crm.P168_place_is_defined_by, node_spaceprimitive))
        g.add((node_spaceprimitive, rdf.type, crm.E94_Space_Primitive))
        g.add(
            (
                node_spaceprimitive,
                crm.P168_place_is_defined_by,
                Literal(
                    (
                        f"Point ( {'+' if place['lng'] > 0 else ''}{place['lng']} {'+' if place['lat'] > 0 else ''}{place['lat']} )"
                    ),
                    datatype=geo.wktLiteral,
                ),
            )
        )
        # define that individual in APIS named graph and APIS entity are the same
    # suggestion for serialization of space primitives according to ISO 6709, to be discussed
    # more place details will be added (references, source, place start date, place end date, relations to other places(?))
    return g


@task(retries=2, retry_delay_seconds=120)
def get_entities_relations_list(
    url: HttpUrl,
    filter_params: dict | None = None,
    entity_type: EntityTypeEnum | None = None,
    api_endpoint: APIEndpointsEnum = "entities",
    mapped_id: int | None = None,
    mapped_filter_key: str | None = None,
    return_results_only: bool = False,
):
    logger = get_run_logger()
    logger.info("retrieving list endpoint")
    if mapped_id is not None and mapped_filter_key is not None:
        filter_params[mapped_filter_key] = mapped_id
    if "?" in url:
        logger.info("base_url not set, trying next_url")
        res = requests.get(url)
    else:
        logger.info("base_url set, composing url and requesting")
        base_params = {"format": "json", "limit": 100}
        url = f"{url}/apis/api/{api_endpoint}/{entity_type}"
        res = requests.get(url, params=ChainMap(filter_params, base_params))
    if res.status_code != 200:
        logger.error(f"Error getting {url}, retrying")
        raise Exception(f"API didnt return 200: {url}")
    else:
        if return_results_only:
            return res.json()["results"]
        return res.json()


@task(retries=2, retry_delay_seconds=60)
def get_entity(entity_id=None, entity_type=None, base_uri=None, url=None):
    """gets organization object from API

    Args:
        organization_id (_type_): _description_
    """
    logger = get_run_logger()
    if entity_id is None:
        logger.warning("Entity is None, skipping")
        return None
    params = {"format": "json", "include_relations": "false"}
    if url is None:
        url = f"{base_uri}/apis/api/entities/{entity_type}/{entity_id}/"
    res = requests.get(url, params=params, headers={"Accept": "application/json"})
    if res.status_code != 200:
        logger.warn(f"Error getting {entity_type}: {res.status_code} / {res.text}")
        raise Exception("API didnt return 200")
    else:
        return res.json()


# @task(max_retries=2, retry_delay_seconds=120)
# def get_entity_relations(base_uri, entity, kind, related_entity_type):
#     """gets entity relations from API

#     Args:
#         person_id (_type_): _description_
#     """
#     logger = get_run_logger()
#     loop_payload = prefect.context.get("task_loop_result", {})
#     if entity is None:
#         logger.info("Entity is None, skipping")
#         raise SKIP
#     elif isinstance(entity, int):
#         entity = {"id": entity}
#     entity_id = entity["id"]
#     url = loop_payload.get("next_url", f"{base_uri}/apis/api/relations/{kind}/")
#     res_full = loop_payload.get("res_full", [])
#     query_params = {f"related_{related_entity_type}": entity_id}
#     if kind[: int(len(kind) / 2)] == kind[int(len(kind) / 2) :]:
#         del query_params[f"related_{related_entity_type}"]
#         query_params[f"related_{related_entity_type}A"] = entity_id
#     res = requests.get(url, params=query_params)
#     if res.status_code != 200:
#         logger.error(f"Error getting {kind} for entity {entity_id}, retrying")
#         raise RETRY
#     res = res.json()
#     res_full.extend(res["results"])
#     if res["next"] is not None:
#         raise LOOP(
#             message=f"offset {res['offset']}",
#             result={"res_full": res_full, "next_url": res["next"]},
#         )
#     return res_full


@task()
def create_base_graph(base_uri):
    global crm
    crm = Namespace("http://www.cidoc-crm.org/cidoc-crm/")
    """Defines namespace for CIDOC CRM."""
    global ex
    ex = Namespace("http://www.intavia.eu/")
    """Defines namespace for own ontology."""
    global idmcore
    idmcore = Namespace("http://www.intavia.eu/idm-core/")
    """Defines namespace for own ontology."""
    global idmrole
    idmrole = Namespace("http://www.intavia.eu/idm-role/")
    """Defines namespace for role ontology."""
    global idmapis
    idmapis = Namespace("http://www.intavia.eu/apis/")
    """Namespace for InTaVia named graph"""
    global idmbibl
    idmbibl = Namespace("http://www.intavia.eu/idm-bibl/")
    """Namespace for bibliographic named graph"""
    global idmrelations
    idmrelations = Namespace("http://www.intavia.eu/idm-relations")
    """Defines namespace for relation ontology."""
    global intavia_shared
    intavia_shared = Namespace("http://www.intavia.eu/shared-entities")
    """Defines namespace for relation ontology."""
    global ore
    ore = Namespace("http://www.openarchives.org/ore/terms/")
    """Defines namespace for schema.org vocabulary."""
    global edm
    edm = Namespace("http://www.europeana.eu/schemas/edm/")
    """Defines namespace for Europeana data model vocabulary."""
    global owl
    owl = Namespace("http://www.w3.org/2002/7/owl#")
    """Defines namespace for Europeana data model vocabulary."""
    global rdf
    rdf = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    """Defines namespace for Europeana data model vocabulary."""
    global xml
    xml = Namespace("http://www.w3.org/XML/1998/namespace")
    """Defines namespace for Europeana data model vocabulary."""
    global xsd
    xsd = Namespace("http://www.w3.org/2001/XMLSchema#")
    """Defines namespace for Europeana data model vocabulary."""
    global bioc
    bioc = Namespace("http://ldf.fi/schema/bioc/")
    """Defines namespace for Europeana data model vocabulary."""
    global rdfs
    rdfs = Namespace("http://www.w3.org/2000/01/rdf-schema#")
    """Defines namespace for Europeana data model vocabulary."""
    global apis
    apis = Namespace(base_uri)
    """Defines namespace for APIS database."""
    global bf
    bf = Namespace("http://id.loc.gov/ontologies/bibframe/")
    """Defines bibframe namespace."""
    global geo
    geo = Namespace("http://www.opengis.net/ont/geosparql#")
    global g
    g = Graph()
    g.bind("apis", apis)
    g.bind("crm", crm)
    g.bind("intaviashared", intavia_shared)
    g.bind("ore", ore)
    g.bind("edm", edm)
    g.bind("owl", owl)
    g.bind("rdf", rdf)
    g.bind("xml", xml)
    g.bind("xsd", xsd)
    g.bind("bioc", bioc)
    g.bind("rdfs", rdfs)
    g.bind("apis", apis)
    g.bind("idmcore", idmcore)
    g.bind("idmrole", idmrole)
    g.bind("idmrelations", idmrelations)
    g.bind("owl", owl)
    g.bind("geo", geo)
    g.bind("bf", bf)
    g.bind("ex", ex)
    g.bind("idmbibl", idmbibl)
    g.bind("idmapis", idmapis)
    global glob_list_entities
    glob_list_entities = {"institutions": [], "persons": [], "places": []}
    return g


@task(tags=["rdf", "serialization"])
def serialize_graph(g, storage_path, add_date_to_file):
    """serializes the RDFLib graph to a given destination

    Args:
        g (rflib): the graph to serialize
        storage_path (str): path within the container to store the file to
        named_graph (uri): optional named graph to serialize

    Returns:
        path: path of the stored file
    """

    Path(storage_path).mkdir(parents=True, exist_ok=True)
    for s, p, o in g.triples((None, bioc.inheres_in, None)):
        g.add((o, bioc.bearer_of, s))
    Path(storage_path).mkdir(parents=True, exist_ok=True)
    f_path = f"{storage_path}/apis_data"
    if add_date_to_file:
        f_path += f"_{datetime.now().strftime('%d-%m-%Y')}"
    f_path += ".ttl"
    g.serialize(
        destination=f_path,
        format="turtle",
    )
    return f_path


# filter_results = FilterTask(
#     filter_func=lambda x: not isinstance(x, (BaseException, SKIP, type(None)))
# )


@task
def upload_data(f_path, named_graph, sparql_endpoint=None):
    logger = get_run_logger()
    data = open(f_path, "rb").read()
    headers = {
        "Content-Type": "application/x-turtle",
    }
    params = {"context-uri": named_graph}
    if sparql_endpoint is None:
        sparql_endpoint = "https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql"
    logger.info(
        f"Uploading data to {sparql_endpoint} with params {params}, loading file from {f_path}"
    )
    upload = requests.post(
        sparql_endpoint,
        data=data,
        headers=headers,
        params=params,
        auth=requests.auth.HTTPBasicAuth(
            os.environ.get("RDFDB_USER"), os.environ.get("RDFDB_PASSWORD")
        ),
    )
    logger.info(f"Upload status: {upload.status_code}")
    if upload.status_code != 200:
        raise Exception(f"Upload failed with status code {upload.status_code}")


class Params(BaseModel):
    max_entities: str = Field(
        None,
        description="Number of entities to retrieve (int). Is just limiting the API requests, mainly used for test purposes",
    )
    named_graph: HttpUrl = Field(
        "http://data.acdh.oeaw.ac.at/apis",
        description="Named graph used to store the data in. Not used if graph is submitted to PR",
    )
    endpoint: HttpUrl = Field(
        "https://apis.acdh.oeaw.ac.at",
        description="Base APIS installation URL to use for sarialization",
    )
    base_uri_serialization: HttpUrl = Field(
        "https://apis.acdh.oeaw.ac.at",
        description="Base URL used to create URIs in the RDF",
    )
    filter_params: dict = Field(
        {"collection": 86},
        description="Filters applied on the first call to the persons API. Default searches for persons depicted in the ÖBL",
    )
    storage_path: DirectoryPath = Field(
        "/archive/serializations/APIS",
        description="Path of the turtle file to use for serialization within the container excuting the flow",
    )
    add_date_to_file: bool = Field(
        False,
        alias="Add date to file",
        description="Whether to add the current date to the file name",
    )
    branch: str = Field(..., description="GIT branch to push to and create PR")
    upload_data: bool = Field(
        False,
        description="Whether to directly upload the data into a configured triplestore",
    )
    push_data_to_repo: bool = Field(
        True,
        description="Whether to push the data to a configured repo and create a PR.",
    )


@flow
def create_apis_rdf_serialization(params: Params):
    logger = get_run_logger()
    g = create_base_graph(params.base_uri_serialization)
    persons = []
    res_persons = get_entities_relations_list(
        url=params.endpoint, filter_params=params.filter_params, entity_type="person"
    )
    persons.extend(res_persons["results"])
    while res_persons["next"] is not None:
        res_persons = get_entities_relations_list(url=res_persons["next"])
        persons.extend(res_persons["results"])
    person_return = render_person.map(
        persons, unmapped(g), unmapped(params.base_uri_serialization)
    )
    pers_id_list = [p["id"] for p in persons]
    pers_inst = []
    pers_inst_res = get_entities_relations_list.map(
        url=params.endpoint,
        filter_params=unmapped({"limit": 1000}),
        entity_type="personinstitution",
        api_endpoint="relations",
        mapped_id=pers_id_list,
        mapped_filter_key="related_person",
        return_results_only=True,
    )
    pers_place_res = get_entities_relations_list.map(
        url=params.endpoint,
        filter_params=unmapped({"limit": 1000}),
        entity_type="personplace",
        api_endpoint="relations",
        mapped_id=pers_id_list,
        mapped_filter_key="related_person",
        return_results_only=True,
    )
    pers_pers_res = get_entities_relations_list.map(
        url=params.endpoint,
        filter_params=unmapped({"limit": 1000}),
        entity_type="personperson",
        api_endpoint="relations",
        mapped_id=pers_id_list,
        mapped_filter_key="related_personA",
        return_results_only=True,
    ) + get_entities_relations_list.map(
        url=params.endpoint,
        filter_params=unmapped({"limit": 1000}),
        entity_type="personperson",
        api_endpoint="relations",
        mapped_id=pers_id_list,
        mapped_filter_key="related_personB",
        return_results_only=True,
    )
    # pers_inst = get_entity_relations.map(
    #     unmapped(params.endpoint),
    #     persons,
    #     kind=unmapped("personinstitution"),
    #     related_entity_type=unmapped("person"),
    # )
    # pers_place = get_entity_relations.map(
    #     unmapped(params.endpoint),
    #     persons,
    #     kind=unmapped("personplace"),
    #     related_entity_type=unmapped("person"),
    # )
    places_data_persons = render_personplace_relation.map(
        list(
            chain.from_iterable(
                f.result() for f in pers_place_res if not f.wait().is_failed()
            )
        ),
        unmapped(g),
        unmapped(params.base_uri_serialization),
    )
    places_data_persons_filtered = list(
        set([p.result() for p in places_data_persons if not p.wait().is_failed()])
    )
    # pers_pers = get_entity_relations.map(
    #     unmapped(params.endpoint),
    #     persons,
    #     kind=unmapped("personperson"),
    #     related_entity_type=unmapped("person"),
    # )
    additional_persons = render_personperson_relation.map(
        list(
            chain.from_iterable(
                f.result()
                for f in pers_pers_res
                if not f.wait().is_failed() and f.result() is not None
            )
        ),
        unmapped(g),
        unmapped(params.base_uri_serialization),
    )
    additional_persons_filtered = list(
        set(
            [
                p.result()
                for p in additional_persons
                if not p.wait().is_failed() and p.result() is not None
            ]
        )
    )

    additional_persons_data = get_entity.map(
        additional_persons_filtered, unmapped("person"), unmapped(params.endpoint)
    )
    additional_persons_return = render_person.map(
        additional_persons_data, unmapped(g), unmapped(params.base_uri_serialization)
    )
    insts = render_personinstitution_relation.map(
        list(
            chain.from_iterable(
                f.result()
                for f in pers_inst_res
                if not f.wait().is_failed() and f.result() is not None
            )
        ),
        unmapped(g),
    )
    outp_persrole = render_personrole_from_relation.map(
        list(
            chain.from_iterable(
                f.result()
                for f in pers_inst_res
                if not f.wait().is_failed() and f.result() is not None
            )
        ),
        unmapped(g),
        unmapped(params.base_uri_serialization),
    )
    outp_persrole_filtered = [
        p.result()
        for p in outp_persrole
        if not p.wait().is_failed() and p.result() is not None
    ]
    additional_roles_data = get_entity.map(url=outp_persrole_filtered)
    additional_roles_return = render_personrole.map(
        additional_roles_data, unmapped(g), unmapped(params.base_uri_serialization)
    )
    insts_filtered = list(
        set(
            [
                p.result()
                for p in insts
                if not p.wait().is_failed() and p.result() is not None
            ]
        )
    )
    insts_data = get_entity.map(
        insts_filtered, unmapped("institution"), unmapped(params.endpoint)
    )
    orga_return = render_organization.map(
        insts_data, unmapped(g), unmapped(params.base_uri_serialization)
    )
    inst_rel_data = get_entities_relations_list.map(
        url=params.endpoint,
        filter_params=unmapped({"limit": 1000}),
        entity_type="institutionplace",
        api_endpoint="relations",
        mapped_id=insts_filtered,
        mapped_filter_key="related_institution",
        return_results_only=True,
    )
    places_data_institutions = render_organizationplace_relation.map(
        list(
            chain.from_iterable(
                f.result()
                for f in inst_rel_data
                if not f.wait().is_failed() and f.result() is not None
            )
        ),
        unmapped(g),
    )
    places_data_institutions_filtered = list(
        set(
            [
                p.result()
                for p in places_data_institutions
                if not p.wait().is_failed() and p.result() is not None
            ]
        )
    )
    places_data_fin = get_entity.map(
        list(set(places_data_institutions + places_data_persons)),
        unmapped("place"),
        unmapped(params.endpoint),
    )
    places_data_fin_filtered = [
        p.result()
        for p in places_data_fin
        if not p.wait().is_failed() and p.result() is not None
    ]
    places_out = render_place.map(
        places_data_fin_filtered, unmapped(g), unmapped(params.base_uri_serialization)
    )
    file_path = serialize_graph.submit(
        g,
        params.storage_path,
        params.add_date_to_file,
        wait_for=[places_out],
    )
    if params.upload_data:
        upload_data(file_path, params.named_graph, wait_for=[file_path])
    if params.push_data_to_repo:
        logger.info(f"Pushing data to repo, using file path: {file_path.result()}")
        push_data_to_repo_flow(
            params=ParamsPush(branch_name=params.branch, file_path=file_path.result())
        )


# state = flow.run(executor=LocalExecutor(), parameters={
#     'Max Entities': 50, 'Filter Parameters': {"collection": 86, "id": 79022}, 'Storage Path': '/workspaces/prefect-flows'})  #
# flow.run_config = KubernetesRun(
#     env={
#         "EXTRA_PIP_PACKAGES": "requests rdflib gitpython",
#     },
#     job_template_path="https://raw.githubusercontent.com/InTaVia/prefect-flows/master/intavia-job-template.yaml",
#     image="ghcr.io/intavia/intavia-prefect-image:1.4.1",
# )
# flow.storage = GitHub(repo="InTaVia/prefect-flows", path="create_apis_graph_v1.py")
if __name__ == "__main__":
    create_apis_rdf_serialization(
        params=Params(
            filter_params={"name": "Streit"},
            storage_path="/workspaces/prefect2-flows/data_serializations",
        )
    )
