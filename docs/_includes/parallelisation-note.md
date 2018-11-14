Most of the time you don't need to parallelize or 
load-balance anything, because Tsp has quite few levels of the
parallelism itself:
1. By partition fields - data level parallelism (SIMD)
2. By patterns - Tsp could divide patterns by their weight (max window) on:
    - Parallel sources - totally separate sources
    - Parallel pattern search branches - to reuse source and not
    overload source DB.