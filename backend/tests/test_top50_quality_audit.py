from backend.app.tools.audit_top50_quality import (
    cluster_descriptions,
    exit_code_from_report,
    jaccard_similarity,
    normalize_text,
    _request_with_retry,
)


def test_normalize_text():
    assert normalize_text("Hello, World!") == "hello world"
    assert normalize_text("  spaced\tout\n") == "spaced out"


def test_jaccard_similarity_basic():
    a = "Foo bar baz"
    b = "foo, baz"
    sim = jaccard_similarity(a, b)
    assert sim == 2 / 3
    assert jaccard_similarity("one", "two") == 0.0


def test_cluster_descriptions():
    descriptions = [
        "Hard-hit hero vs lefties",
        "hard hit hero vs lefties!",
        "Totally different blurb",
        "Another unique statement",
        "Another unique statement!",  # duplicate-ish
    ]
    clusters = cluster_descriptions(descriptions, threshold=0.8)
    # Expect two clusters: first two; last two.
    sizes = sorted([len(c) for c in clusters], reverse=True)
    assert sizes[0] == 2
    assert sizes[1] == 2


def test_request_with_retry(monkeypatch):
    calls = {"count": 0}

    class DummyResp:
        def __init__(self, status_code=200):
            self.status_code = status_code

        def raise_for_status(self):
            if self.status_code >= 400:
                import requests
                raise requests.HTTPError(f"status {self.status_code}", response=self)

        def json(self):
            return {"ok": True}

    def fake_get(url, timeout):
        calls["count"] += 1
        if calls["count"] < 3:
            import requests
            raise requests.Timeout("timeout")
        return DummyResp(200)

    monkeypatch.setattr("requests.get", fake_get)
    resp = _request_with_retry("http://example.com", timeout=0.1, retries=3, backoff=0.001)
    assert resp.json() == {"ok": True}
    assert calls["count"] == 3


def test_exit_code_from_report():
    clean_report = {
        "overall_verdict": "PASS",
        "days": {"2025-05-01": {"verdict": "PASS"}, "2025-05-02": {"verdict": "PASS"}},
    }
    assert exit_code_from_report(clean_report) == 0

    overall_fail = {**clean_report, "overall_verdict": "FAIL"}
    assert exit_code_from_report(overall_fail) == 1

    day_fail = {
        "overall_verdict": "PASS",
        "days": {"2025-05-01": {"verdict": "FAIL"}, "2025-05-02": {"verdict": "PASS"}},
    }
    assert exit_code_from_report(day_fail) == 1
