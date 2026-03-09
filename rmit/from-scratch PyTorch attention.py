import torch
import torch.nn as nn
import torch.nn.functional as F

torch.manual_seed(0)

# ---- pretend these are token embeddings ----
B, T, d_model = 1, 4, 8          # batch, sequence length, embedding size
x = torch.randn(B, T, d_model)   # (1, 4, 8)

# ---- choose attention head size ----
d_k = 4

# ---- separate learned projections (the key point!) ----
W_Q = nn.Linear(d_model, d_k, bias=False)
W_K = nn.Linear(d_model, d_k, bias=False)
W_V = nn.Linear(d_model, d_k, bias=False)

Q = W_Q(x)   # (B, T, d_k)
K = W_K(x)   # (B, T, d_k)
V = W_V(x)   # (B, T, d_k)

# ---- scaled dot-product attention ----
scores = Q @ K.transpose(-2, -1) / (d_k ** 0.5)   # (B, T, T)
attn = F.softmax(scores, dim=-1)                  # (B, T, T) rows sum to 1
out = attn @ V                                    # (B, T, d_k)

print("x:", x.shape)
print("Q,K,V:", Q.shape, K.shape, V.shape)
print("scores:", scores.shape)
print("attn:", attn.shape, "row sums:", attn[0].sum(dim=-1))
print("out:", out.shape)

import torch
import torch.nn as nn
import torch.nn.functional as F

torch.manual_seed(0)

# ---- pretend these are token embeddings ----
B, T, d_model = 1, 4, 8          # batch, sequence length, embedding size
x = torch.randn(B, T, d_model)   # (1, 4, 8)

# ---- choose attention head size ----
d_k = 4

# ---- separate learned projections (the key point!) ----
W_Q = nn.Linear(d_model, d_k, bias=False)
W_K = nn.Linear(d_model, d_k, bias=False)
W_V = nn.Linear(d_model, d_k, bias=False)

Q = W_Q(x)   # (B, T, d_k)
K = W_K(x)   # (B, T, d_k)
V = W_V(x)   # (B, T, d_k)

# ---- scaled dot-product attention ----
scores = Q @ K.transpose(-2, -1) / (d_k ** 0.5)   # (B, T, T)
attn = F.softmax(scores, dim=-1)                  # (B, T, T) rows sum to 1
out = attn @ V                                    # (B, T, d_k)

print("x:", x.shape)
print("Q,K,V:", Q.shape, K.shape, V.shape)
print("scores:", scores.shape)
print("attn:", attn.shape, "row sums:", attn[0].sum(dim=-1))
print("out:", out.shape)

print("\nAttention weights (token 0 attending to all tokens):")
print(attn[0, 0])   # 4 numbers, sums to 1