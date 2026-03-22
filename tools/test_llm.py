"""Simple script to verify the optional LLM integration works.

Usage: activate the venv, install the extra requirements, and point
``LLM_MODEL_PATH`` at a downloaded q4 model file.

It will initialise the model and run a couple of sample prompts.
"""
import os
import sys
import asyncio

# make sure the bot package is importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core import llm


def main():
    print("backend available before init:", llm._backend)
    try:
        llm.init_llm()
    except Exception as e:
        print("initialisation failed:", e)
        return
    print("backend selected:", llm._backend)

    prompt = "Write a friendly greeting for a Telegram post."
    print("prompt:", prompt)
    try:
        result = llm.generate(prompt)
        print("sync result:\n", result)
    except Exception as exc:
        print("sync generation error", exc)

    async def test_async():
        out = await llm.generate_async(prompt)
        print("async result:\n", out)

    asyncio.run(test_async())


if __name__ == "__main__":
    main()
