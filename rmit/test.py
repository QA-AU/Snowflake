import torch

print("Torch version:", torch.__version__)
print("MPS Available:", torch.backends.mps.is_available())
