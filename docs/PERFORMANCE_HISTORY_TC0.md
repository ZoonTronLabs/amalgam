# Withdrawn TC=0 performance history

Early comparisons forced DOTNET_TieredCompilation=0 and used insufficient warmup.
They do not establish performance against production-default FusionCache.
Raw exploratory history was archived before cleanup and is excluded from the
crate. The maintained [performance summary](PERFORMANCE.md) uses FC tiering/PGO,
separate operating systems and explicit failed budgets.
