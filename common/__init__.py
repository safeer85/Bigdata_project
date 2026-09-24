"""Shared logic imported by BOTH the speed layer and the batch layer.

Lambda architecture's best-known weakness is that the same business rule ends up
written twice -- once for streaming and once for batch -- and the two copies drift
apart. Everything that both layers need (the simulated clock, zone lookup, the
event schema, validation rules, fare/earnings maths and the profitability rules)
lives in this package and is imported by both. That is our mitigation, and we
defend it in the report.
"""

__version__ = "1.0.0"
