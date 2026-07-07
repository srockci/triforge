"""TriForge-style multi-agent workflow server.

Exposes a FastAPI server with /workflow/start, /status, /approve endpoints
that Hermes (Telegram coordinator) calls to run the modular pipeline:
Architect-A designs → module_detail/code/test loop → Architect-A reviews.
"""