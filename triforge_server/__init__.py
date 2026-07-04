"""TriForge-style multi-agent workflow server.

Exposes a FastAPI server with /workflow/start, /status, /approve endpoints
that Hermes (Telegram coordinator) calls to run A -> B -> A pipelines
(Architect-A designs, Coder-B implements, Architect-A reviews).
"""