import numpy as np


# ----------------------------------------------------------
# Sigmoid Activation Function
# ----------------------------------------------------------
# This function introduces NON-LINEARITY into the network.
#
# Formula:
#     sigmoid(x) = 1 / (1 + e^(-x))
#
# Why we need it:
# - Converts any number into a value between 0 and 1
# - Allows the network to learn nonlinear patterns
# - Without this, the entire network becomes linear
# ----------------------------------------------------------
def sigmoid(x):
    return 1 / (1 + np.exp(-x))


# ----------------------------------------------------------
# Network Architecture Definition
# ----------------------------------------------------------
# N_input  = number of input features
# N_hidden = number of neurons in hidden layer
# N_output = number of output neurons
#
# This network structure is:
#     4 inputs → 3 hidden neurons → 2 output neurons
# ----------------------------------------------------------
N_input = 4
N_hidden = 3
N_output = 2


# ----------------------------------------------------------
# Set Random Seed
# ----------------------------------------------------------
# Ensures reproducibility.
# Every time this runs, weights and data will be the same.
# Useful for debugging and grading.
# ----------------------------------------------------------
np.random.seed(42)


# ----------------------------------------------------------
# Create Fake Input Data
# ----------------------------------------------------------
# X is a single input sample with 4 features.
# Shape: (4,)
#
# Example meaning (conceptually):
# X = [feature1, feature2, feature3, feature4]
# ----------------------------------------------------------
X = np.random.randn(4)


# ----------------------------------------------------------
# Initialize Weights: Input → Hidden
# ----------------------------------------------------------
# Shape: (4, 3)
#
# Each column represents all weights feeding into
# one hidden neuron.
#
# Row = input feature
# Column = hidden neuron
#
# Small random values are used to:
# - Prevent symmetry
# - Avoid exploding activations
# ----------------------------------------------------------
weights_input_to_hidden = np.random.normal(0, scale=0.1, size=(N_input, N_hidden))


# ----------------------------------------------------------
# Initialize Weights: Hidden → Output
# ----------------------------------------------------------
# Shape: (3, 2)
#
# Each column represents all weights feeding into
# one output neuron.
#
# Row = hidden neuron
# Column = output neuron
# ----------------------------------------------------------
weights_hidden_to_output = np.random.normal(0, scale=0.1, size=(N_hidden, N_output))


# ==========================================================
#               FORWARD PASS STARTS HERE
# ==========================================================
# Forward pass means:
#     Move information from input → hidden → output
#     No learning happens here.
#     Just computing prediction.
# ==========================================================


# ----------------------------------------------------------
# Step 1: Compute Hidden Layer Input
# ----------------------------------------------------------
# Matrix multiplication:
#   (4,) dot (4×3) → (3,)
#
# This follows all arrows from input nodes to hidden nodes.
#
# For each hidden neuron j:
#   h_j = x1*w1j + x2*w2j + x3*w3j + x4*w4j
#
# Result:
#   hidden_layer_in is a vector of length 3
# ----------------------------------------------------------
hidden_layer_in = np.dot(X, weights_input_to_hidden)


# ----------------------------------------------------------
# Step 2: Apply Activation to Hidden Layer
# ----------------------------------------------------------
# Apply sigmoid element-wise.
#
# This makes the model NON-LINEAR.
# Without this step, the network collapses into
# a single linear transformation.
#
# Output shape remains (3,)
# ----------------------------------------------------------
hidden_layer_out = sigmoid(hidden_layer_in)

print("Hidden-layer Output:")
print(hidden_layer_out)


# ----------------------------------------------------------
# Step 3: Compute Output Layer Input
# ----------------------------------------------------------
# Matrix multiplication:
#   (3,) dot (3×2) → (2,)
#
# Each output neuron receives weighted signals
# from all hidden neurons.
#
# For each output neuron k:
#   o_k = h1*w1k + h2*w2k + h3*w3k
#
# Result:
#   output_layer_in is a vector of length 2
# ----------------------------------------------------------
output_layer_in = np.dot(hidden_layer_out, weights_hidden_to_output)


# ----------------------------------------------------------
# Step 4: Apply Activation to Output Layer
# ----------------------------------------------------------
# Converts final signals into probabilities (0–1).
#
# Final network output.
# Shape: (2,)
# ----------------------------------------------------------
output_layer_out = sigmoid(output_layer_in)

print("Output-layer Output:")
print(output_layer_out)
