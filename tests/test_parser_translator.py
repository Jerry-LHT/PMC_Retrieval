from parser.pubmed_parser import PubMedParser
from parser.translator import OpenSearchTranslator, Weights


def test_parser_boolean_structure() -> None:
    parser = PubMedParser()
    ast = parser.parse('("heart failure"[ti] AND pregnancy[tiab]) OR PMC12345')
    assert ast is not None


def test_translator_pmcid() -> None:
    parser = PubMedParser()
    ast = parser.parse("PMC11458033")
    dsl = OpenSearchTranslator().translate(ast)
    should = dsl["bool"]["should"]
    assert should[0]["term"]["pmcid"] == "PMC11458033"


def test_translator_date_field_requires_exists() -> None:
    parser = PubMedParser()
    ast = parser.parse("2024-10-07[dp]")
    dsl = OpenSearchTranslator().translate(ast)
    must = dsl["bool"]["must"]
    assert {"exists": {"field": "publication_date"}} in must


def test_translator_parenthesized_expression() -> None:
    parser = PubMedParser()
    ast = parser.parse("(fertilization[tiab] AND China[tiab]) OR PMC11458033")
    dsl = OpenSearchTranslator().translate(ast)
    should = dsl["bool"]["should"]
    assert should[0]["bool"]["must"]
    assert should[1]["bool"]["should"]


def test_translator_title_prefix_uses_normalized_prefix_query() -> None:
    parser = PubMedParser()
    ast = parser.parse("ferti*[ti]")
    dsl = OpenSearchTranslator().translate(ast)
    assert dsl == {"prefix": {"title_normalized": {"value": "ferti"}}}


def test_translator_default_query_skips_full_text_when_weight_is_zero() -> None:
    parser = PubMedParser()
    ast = parser.parse("fertilization")
    dsl = OpenSearchTranslator(weights=Weights(full_text_clean=0)).translate(ast)
    fields = dsl["multi_match"]["fields"]
    assert not any(field.startswith("full_text_clean^") for field in fields)
