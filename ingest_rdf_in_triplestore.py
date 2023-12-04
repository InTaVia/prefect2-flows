from pydantic import BaseModel, Field, HttpUrl


class Params(BaseModel):
    url: HttpUrl = Field(..., description="URL of the triplestore")
    named_graph: HttpUrl = Field(..., description="URL of the named graph")
