from prefect import flow, task
from pydantic import BaseModel, Field, HttpUrl
from SPARQLWrapper import SPARQLWrapper, JSON
from prefect.blocks.system import Secret, String


@task(tags=["triplestore"], reties=3, retry_dely_seconds=30)
def query_data_from_triplestore(url, username, password, query, flatten_return):
    """Query data from a triplestore

    Args:
        url (HttpUrl): URL of the triplestore
        username (str): Username for the triplestore
        password (str): Password for the triplestore
        query (str): SPARQL query to execute
        flatten_return (bool): Whether to flatten the return value

    Returns:
        dict: Query result
    """

    sparql = SPARQLWrapper(url)
    sparql.setHTTPAuth("BASIC")
    sparql.setCredentials(username, password)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    if flatten_return:
        return [item for result in results["results"]["bindings"] for item in result]
    else:
        return results["results"]["bindings"]


class Params(BaseModel):
    ts_url: HttpUrl = Field(..., description="URL of the triplestore")
    ts_username: str = Field(
        ..., description="Prefect String block holding the username for the triplestore"
    )
    ts_password: str = Field(
        ..., description="Prefect Secret holding the password for the triplestore"
    )
    flatten_return: bool = Field(
        False, description="Whether to flatten the return value"
    )
    query: str = Field(..., description="SPARQL query to execute")


@flow
def query_data_from_triplestore_flow(params: Params):
    username = String.load(params.ts_username).get()
    password = Secret.load(params.ts_password).get()
    return query_data_from_triplestore(
        params.ts_url,
        username,
        password,
        params.query,
        params.flatten_return,
    )
