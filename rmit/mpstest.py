import torch

device = torch.device("mps")
x = torch.randn(1000, 1000, device=device)
y = torch.matmul(x, x)

print("Running on:", device)
