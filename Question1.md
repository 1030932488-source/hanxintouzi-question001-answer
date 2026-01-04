# Question 1: Reverse Symmetric Matrix Algorithm

## Algorithm Design

The goal is to compute the matrix-vector product $A \cdot v$, where $A$ is a reverse symmetric matrix.
A reverse symmetric matrix has the property that $A_{i,j} = a_{n-1+i-j}$.

The product $(A \cdot v)_i = \sum_{j=0}^{n-1} A_{i,j} \cdot v_j = \sum_{j=0}^{n-1} a_{n-1+i-j} \cdot v_j$.

### Compressed Storage

The matrix is determined by $2n-1$ values: $a_0, \dots, a_{2n-2}$. We store these in an array `a`.

### Algorithm (O(n^2) Time, O(n) Space)

We can compute the result directly without constructing the full matrix.

```python
def reverse_symmetric_multiply_naive(a_compressed, v):
    n = len(v)
    result = [0.0] * n
    for i in range(n):
        for j in range(n):
            k = n - 1 + i - j
            result[i] += a_compressed[k] * v[j]
    return result
```

### Optimized Algorithm (O(n log n) Time)

The operation is a convolution. Specifically, it is a Toeplitz matrix-vector multiplication (or Hankel, depending on indexing).
Note that $(A \cdot v)_i$ is the coefficient of index $i$ in a convolution of $a$ and $v$ if properly reversed/padded.
Using Fast Fourier Transform (FFT), we can compute convolution in $O(n \log n)$.

1. Construct polynomial $P_v(x) = \sum v_j x^j$.
2. Construct polynomial $P_a(x) = \sum a_k x^k$.
3. Multiply polynomials using FFT.
4. Extract coefficients corresponding to the matrix-vector product indices.

## Complexity Analysis

- **Time Complexity**:
  - Naive: $O(n^2)$ due to nested loops.
  - FFT-based: $O(n \log n)$.
- **Space Complexity**: $O(n)$ to store input/output vectors and the compressed matrix array.
