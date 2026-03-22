"""
Light wrappers for local LLM inference engines.

This module provides a very small, lazily-initialised interface that the
rest of the bot can import without caring which backend is actually being
used.  The intent is to load a CPU-friendly, quantised model (ggml, gpt4all,
etc.) and expose both synchronous and asynchronous helpers.

Environment variables that can be used to control behaviour:

* ``LLM_MODEL_PATH`` – path to the model file (default ``models/ggml-alpaca-7b-q4.bin``).
* ``LLM_THREADS`` – number of CPU threads to use when initialising the model.
* ``USE_LLM_BACKEND`` – if set to ``"llama"`` or ``"gpt4all"`` will explicitly
  select the corresponding backend; otherwise the first available backend
  will be picked.
* ``USE_LLM_CAPTION`` – used by :mod:`main` to decide whether to post-process
  captions (this module itself does not look at that variable).

"""
import os
import asyncio
from typing import Any, Optional

# backend flags ----------------------------------------------------------------
_llama_available = False
_airllm_available = False
_gpt4all_available = False

try:
    from llama_cpp import Llama  # type: ignore[import]
    _llama_available = True
except ImportError:  # pragma: no cover - might not be installed
    pass

try:
    # airllm is optional; its API is slightly different (LLM.load)
    from airllm import LLM as AirLLM  # type: ignore[import]
    _airllm_available = True
except ImportError:  # pragma: no cover
    pass

try:
    import gpt4all  # type: ignore[import]  # noqa: F401 - imports register backend
    _gpt4all_available = True
except ImportError:  # pragma: no cover
    pass

_model: Any = None
_backend: Optional[str] = None


def _choose_backend() -> str:
    """Decide which backend to use based on availability and env vars."""
    # explicit override
    explicit = os.getenv("USE_LLM_BACKEND", "").strip().lower()
    if explicit:
        if explicit in ("llama",) and _llama_available:
            return "llama"
        if explicit in ("gpt4all",) and _gpt4all_available:
            return "gpt4all"
        if explicit in ("airllm",) and _airllm_available:
            return "airllm"
        # fall through and let auto-selection handle missing backend

    # pick first available
    if _llama_available:
        return "llama"
    if _gpt4all_available:
        return "gpt4all"
    if _airllm_available:
        return "airllm"
    raise RuntimeError("no LLM backend available, install llama-cpp-python or airllm")


def init_llm(model_path: Optional[str] = None) -> Any:
    """Initialise and return a global LLM instance.

    The model is created lazily the first time this function is called.  The
    returned object has a simple callable API: ``model(prompt, **kwargs)`` and
    returns a dict with a ``choices`` list matching the behaviour of the
    OpenAI schema (this mirrors ``llama-cpp-python``).  ``generate`` and
    ``generate_async`` wrap this output.
    """
    global _model, _backend
    if _model is not None:
        return _model

    backend = _choose_backend()
    _backend = backend

    if model_path is None:
        model_path = os.getenv("LLM_MODEL_PATH", "models/ggml-alpaca-7b-q4.bin")

    threads = int(os.getenv("LLM_THREADS", "4"))

    if backend == "llama":
        # llama-cpp-python provides a simple constructor
        _model = Llama(model_path=model_path, n_threads=threads)
    elif backend == "gpt4all":
        # gpt4all uses a factory that returns a model object
        from gpt4all import GPT4All  # type: ignore[import]

        _model = GPT4All(model=model_path, n_threads=threads)
    elif backend == "airllm":
        # airllm uses LLM.load
        _model = AirLLM.load(model_path, backend="llama.cpp", n_threads=threads)
    else:  # pragma: no cover - defensive
        raise RuntimeError(f"unsupported backend {backend}")

    return _model


def generate(prompt: str, **kwargs: Any) -> str:
    """Run inference synchronously and return the generated text.

    Additional ``kwargs`` are forwarded to the underlying model call (for
    instance, ``max_tokens``).
    """
    model = init_llm()
    out = model(prompt, **kwargs)
    # llama-cpp-python and friends return {'choices': [{'text': ...}]}
    try:
        return out["choices"][0]["text"]
    except Exception:  # pragma: no cover - best effort
        return str(out)


def generate_async(prompt: str, **kwargs: Any) -> "asyncio.Future[str]":
    """Asynchronous wrapper around :func:`generate`.

    This simply delegates to ``asyncio.to_thread`` so the CPU-heavy work
    doesn't block the event loop.
    """
    return asyncio.to_thread(generate, prompt, **kwargs)
