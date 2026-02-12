from __future__ import annotations

from types import SimpleNamespace

from basalt import polars_engine


class _FakeLazyFrame:
    def __init__(self):
        self.calls = []

    def collect(self, **kwargs):
        self.calls.append(kwargs)
        return {"ok": True, "kwargs": kwargs}


def test_should_use_gpu_respects_explicit_flag(monkeypatch):
    monkeypatch.setattr(polars_engine, "has_nvidia_gpu", lambda: True)
    assert polars_engine.should_use_gpu(SimpleNamespace(POLARS_GPU_ENABLED=True)) is True
    assert polars_engine.should_use_gpu(SimpleNamespace(POLARS_GPU_ENABLED=False)) is False


def test_should_use_gpu_auto_detect(monkeypatch):
    monkeypatch.setattr(polars_engine, "has_nvidia_gpu", lambda: True)
    assert polars_engine.should_use_gpu(SimpleNamespace(POLARS_GPU_ENABLED=None)) is True
    monkeypatch.setattr(polars_engine, "has_nvidia_gpu", lambda: False)
    assert polars_engine.should_use_gpu(SimpleNamespace(POLARS_GPU_ENABLED=None)) is False


def test_collect_lazy_prefers_gpu_engine(monkeypatch):
    lf = _FakeLazyFrame()
    monkeypatch.setattr(polars_engine, "should_use_gpu", lambda _cfg: True)
    monkeypatch.setattr(polars_engine, "build_gpu_engine", lambda: "gpu-engine")
    out = polars_engine.collect_lazy(lf, config=SimpleNamespace())
    assert out["ok"] is True
    assert lf.calls == [{"engine": "gpu-engine"}]


def test_collect_lazy_falls_back_to_streaming_cpu(monkeypatch):
    lf = _FakeLazyFrame()
    monkeypatch.setattr(polars_engine, "should_use_gpu", lambda _cfg: False)
    out = polars_engine.collect_lazy(lf, config=SimpleNamespace(), streaming=True)
    assert out["ok"] is True
    assert lf.calls == [{"engine": "streaming"}]
