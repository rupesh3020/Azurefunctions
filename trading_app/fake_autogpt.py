import random

class FakeAutoGPT:
    """Simple AutoGPT-like class generating random trading suggestions."""

    def __init__(self, seed: int | None = None) -> None:
        self.random = random.Random(seed)

    def generate_signal(self, symbol: str) -> str:
        """Return a random suggestion for the given symbol."""
        return self.random.choice(["buy", "sell", "hold"])
