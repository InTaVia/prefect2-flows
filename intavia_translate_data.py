from prefect import flow, task, unmapped
from pydantic import BaseModel, Field
from prefect.blocks.system import Secret
import deepl

from enum import Enum


class LangEnum(Enum):
    en = "en"
    de = "de"
    fi = "fi"
    nl = "nl"
    sl = "sl"


@task(tags=["deepl"], reties=3, retry_dely_seconds=30)
def translate_term(term, translator, source_lang, target_lang):
    return translator.translate_text(
        term, target_lang=target_lang.upper(), source_lang=source_lang.upper()
    )


class Params(BaseModel):
    deepl_api_key: str = Field(
        ..., description="Prefect Secret holding the API key for DeepL"
    )
    source_lang: LangEnum = Field(..., description="Source language")
    target_lang: LangEnum = Field(..., description="Target language")
    terms: list[str] = Field(..., description="List of terms to translate")


@flow
def translate_data_flow(params: Params):
    api_key = Secret.load(params.deepl_api_key).get()
    translator = deepl.Translator(api_key)
    return translate_term.map(
        params.terms, unmapped(translator), params.source_lang, params.target_lang
    )
