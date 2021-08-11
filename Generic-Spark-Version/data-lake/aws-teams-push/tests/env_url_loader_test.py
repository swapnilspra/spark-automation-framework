from src.env_url_loader import EnvUrlLoader 

SAMPLE_ENV = {
    "TEAMS_HOOK_1": "https://example.com/one",
    "TEAMS_HOOK_2": "https://example.com/two",
    "NOT_TEAMS_HOOK_1": "https://example.com/not",
    "TEAMS_HOOKNOT": "https://example.com/not",
    "Teams_Hook_mixedcase": "https://example.com/not",
}

def test_filter():
    actual = EnvUrlLoader.extract(src = SAMPLE_ENV, prefix='TEAMS_HOOK')
    assert len(actual) == 2
