# Question 2: FFT Regex Matching

## Problem

Given a main string $S$ (length $n$) and a pattern string $P$ (length $m$) containing wildcards `?`. Find all matches.

## Algorithm using FFT

We can use FFT to solve this pattern matching problem with wildcards.

### Modeling

We want to find index $i$ such that for all $j \in [0, m-1]$, $S[i+j]$ matches $P[j]$.
Match condition: $(S[i+j] - P[j])^2 P[j] S[i+j] ...$ simplified:
A mismatch occurs if character codes differ, unless one is `?`.
Let's map characters to numbers (a=1, b=2...). `?` = 0.
The matching function for position $i$:
$$ \sum_{j=0}^{m-1} P[j] (S[i+j] - P[j])^2 = 0 $$
Wait, this works if `?` is 0 in $P$, assuming $S$ has no `?`.
If $P[j] = 0$ (wildcard), the term becomes 0 regardless of $S[i+j]$.
So the condition $\sum_{j=0}^{m-1} P[j] (S[i+j] - P[j])^2 = 0$ works to detect matches (sum will be 0 iff all non-wildcards match).

Expand the term:
$$ \sum P[j] (S[i+j]^2 - 2 S[i+j] P[j] + P[j]^2) $$
$$ = \sum P[j] S[i+j]^2 - 2 \sum P[j]^2 S[i+j] + \sum P[j]^3 $$

These are three convolutions!

1. Convolution of $P$ (reversed) and $S^2$.
2. Convolution of $P^2$ (reversed) and $S$.
3. Sum of $P^3$ (constant for each window, but actually can be seen as sum of $P^3$ terms involving valid overlaps). Actually, $\sum P[j]^3$ depends only on $P$, not $i$, if computed over valid range.

### Steps

1. Map alphabet to integers. Map `?` to 0.
2. Construct arrays:
   - $S_1 = S, S_2 = S^2, S_3 = S^3$ (Use $S_1$ and $S_2$ here).
   - $P_1 = P, P_2 = P^2, P_3 = P^3$.
   - Reverse $P$ arrays.
3. Compute 3 convolutions using FFT:
   - $C_1 = P \ast S^2$
   - $C_2 = P^2 \ast S$
   - $C_3 = \text{Sum}(P^3)$ (Just a scalar sum? No, we need it for the window. Actually it's $\sum_{j=0}^{m-1} P[j]^3$, which is a constant $K$ if $P$ is fixed).
   Wait, the formula is $\sum_{j=0}^{m-1} (P[j] S[i+j]^2 - 2 P[j]^2 S[i+j] + P[j]^3)$.
   So we need:
   - $A = \text{convolution}(P, S^2)$
   - $B = \text{convolution}(P^2, S)$
   - $C = \sum_{j=0}^{m-1} P[j]^3$ (Constant).
   Result at $i$: $A[i] - 2B[i] + C$.
4. If Result$[i] == 0$ (approximate for float errors), then match at $i$.

## Complexity

- $O(n \log n)$ time for FFT.

## Optimization

- Use Number Theoretic Transform (NTT) to avoid precision issues.
- If alphabet is small, use bitsets ($O(nm/w)$).
